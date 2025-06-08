"""
File system monitoring service for S3 File Upload Service.

This module provides async monitoring of directories for file changes.
It periodically scans directories from upload requests and detects
new, modified, or deleted files based on their paths and modification times.
"""

import asyncio
import glob
import logging
import os
import time  # Import time module for Unix timestamps
from typing import Dict, Set, Tuple

from app.db import db
from app.config import config
from app.tasks import create_file_task, update_upload_request_status

logger = logging.getLogger(__name__)


class FileMonitor:
    """
    Monitor directories for file changes.

    This class maintains a record of files in monitored directories
    and periodically checks for changes.
    """

    def __init__(self, scan_interval: int = config.probe_interval):
        """
        Initialize the file monitor.

        Args:
            scan_interval: Interval between directory scans in seconds
        """
        self.scan_interval = scan_interval
        # Maps upload_id to a dict of file paths and their last modified times
        self.tracked_files: Dict[str, Dict[str, float]] = {}
        self._running = False
        self._task = None

    async def start(self):
        """Start the file monitoring service."""
        if self._running:
            logger.warning("File monitor already running")
            return

        self._running = True
        self._task = asyncio.create_task(self._monitoring_loop())
        logger.info("File monitoring service started")

    async def stop(self):
        """Stop the file monitoring service."""
        if not self._running:
            logger.warning("File monitor not running")
            return

        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        logger.info("File monitoring service stopped")

    async def _monitoring_loop(self):
        """Main monitoring loop that runs continuously."""
        while self._running:
            try:
                await self._scan_all_directories()
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")

            # Wait for the next scan interval
            await asyncio.sleep(self.scan_interval)

    async def _scan_all_directories(self):
        """Scan all directories from active upload requests."""
        # Get all active upload requests
        uploads = await db.get_all_upload_requests()
        active_upload_ids = set()

        for upload in uploads:
            upload_id = upload["upload_id"]
            active_upload_ids.add(upload_id)

            # Skip if upload is cancelled or in error state
            # Only "pending" and "active" are valid states for monitoring
            if upload["status"] not in ["pending", "active"]:
                logger.debug(f"Skipping scan for upload {upload_id} with status {upload['status']}")
                continue

            # Scan the directory for this upload
            await self._scan_directory(upload_id, upload["source_folder"], upload["pattern"])

        # Remove tracking for uploads that are no longer active
        for upload_id in list(self.tracked_files.keys()):
            if upload_id not in active_upload_ids:
                logger.debug(f"Removing tracking for inactive upload {upload_id}")
                self.tracked_files.pop(upload_id, None)

    async def _scan_directory(self, upload_id: str, directory: str, pattern: str):
        """
        Scan a directory for file changes.

        Args:
            upload_id: The ID of the upload request
            directory: The directory to scan
            pattern: The glob pattern to match files
        """
        logger.debug(f"Scanning directory {directory} with pattern {pattern} for upload {upload_id}")

        # Ensure the directory exists
        if not os.path.isdir(directory):
            logger.warning(f"Directory {directory} does not exist for upload {upload_id}")
            return

        # Get all files matching the pattern
        full_pattern = os.path.join(directory, pattern)
        current_files = {}

        for file_path in glob.glob(full_pattern, recursive=True):
            if os.path.isfile(file_path):
                # Get last modified time
                mod_time = os.path.getmtime(file_path)
                current_files[file_path] = mod_time

        # Get previously tracked files for this upload
        previous_files = self.tracked_files.get(upload_id, {})

        # Detect changes
        new_files, modified_files, deleted_files = self._detect_changes(previous_files, current_files)

        # Log changes
        for file_path in new_files:
            logger.debug(f"New file detected: {file_path} for upload {upload_id}")

        for file_path in modified_files:
            logger.debug(f"Modified file detected: {file_path} for upload {upload_id}")

        for file_path in deleted_files:
            logger.debug(f"Deleted file detected: {file_path} for upload {upload_id}")

        # Generate file tasks if enough changes have accumulated
        await self._process_changes(upload_id, directory, new_files, modified_files)

        # Update tracked files
        self.tracked_files[upload_id] = current_files

    def _detect_changes(
        self, previous_files: Dict[str, float], current_files: Dict[str, float]
    ) -> Tuple[Set[str], Set[str], Set[str]]:
        """
        Detect file changes between scans.

        Args:
            previous_files: Dict of file paths to modification times from previous scan
            current_files: Dict of file paths to modification times from current scan

        Returns:
            A tuple of sets containing (new_files, modified_files, deleted_files)
        """
        previous_paths = set(previous_files.keys())
        current_paths = set(current_files.keys())

        # New files are in current but not in previous
        new_files = current_paths - previous_paths

        # Deleted files are in previous but not in current
        deleted_files = previous_paths - current_paths

        # Modified files are in both but have different modification times
        modified_files = set()
        for path in previous_paths.intersection(current_paths):
            if previous_files[path] != current_files[path]:
                modified_files.add(path)

        return new_files, modified_files, deleted_files

    async def _process_changes(self, upload_id: str, source_folder: str, new_files: Set[str], modified_files: Set[str]):
        """
        Process detected changes and generate file tasks when enough changes have accumulated.

        Args:
            upload_id: The ID of the upload request
            source_folder: The source folder of the upload request
            new_files: Set of new files detected
            modified_files: Set of modified files detected
        """
        # Skip if no changes
        if not new_files and not modified_files:
            # If there are no changes but we were tracking changes for this upload.
            return

        # Get the current time as Unix timestamp (same units as file modification times)
        current_timestamp = time.time()

        # Track all changes and calculate total size
        total_change_size_mb = 0
        all_changed_files = []
        files_overdue = False

        # Process new files
        for file_path in new_files:
            try:
                file_size_mb = os.path.getsize(file_path) / (1024 * 1024)  # Convert to MB
                total_change_size_mb += file_size_mb
                all_changed_files.append(file_path)
            except (OSError, FileNotFoundError) as e:
                logger.warning(f"Error getting size of new file {file_path}: {e}")

        # Process modified files
        for file_path in modified_files:
            try:
                file_size_mb = os.path.getsize(file_path) / (1024 * 1024)  # Convert to MB
                total_change_size_mb += file_size_mb
                all_changed_files.append(file_path)
            except (OSError, FileNotFoundError) as e:
                logger.warning(f"Error getting size of modified file {file_path}: {e}")

        # If there are no valid files to process, return early
        if not all_changed_files:
            return

        # Get thresholds from config
        probe_chunk_size_mb = config.probe_chunk_size_mb
        probe_time_threshold = config.probe_time_threshold

        # Check each file to see if it's been waiting too long based on its modification time
        for file_path in all_changed_files:
            try:
                if os.path.exists(file_path):
                    # Get file modification time
                    mod_time = os.path.getmtime(file_path)
                    file_age = current_timestamp - mod_time

                    if file_age >= probe_time_threshold:
                        # This file has been modified long enough ago to be considered overdue
                        files_overdue = True
                        logger.debug(
                            f"File {file_path} is {file_age:.2f} seconds old, "
                            f"exceeding threshold of {probe_time_threshold} seconds"
                        )
            except (OSError, FileNotFoundError) as e:
                logger.warning(f"Error checking modification time for file {file_path}: {e}")

        # Determine if we should generate tasks based on overall size or individual file time
        size_threshold_met = total_change_size_mb >= probe_chunk_size_mb

        # If any of the thresholds are met, generate tasks
        if size_threshold_met or files_overdue:
            # Determine which files to process
            # Process all changed files
            all_changed_files
            if size_threshold_met:
                logger.info(
                    f"Generating tasks for upload {upload_id} due to size threshold: "
                    f"{len(all_changed_files)} files with total size {total_change_size_mb:.2f} MB "
                    f"(threshold: {probe_chunk_size_mb} MB)"
                )
            else:
                logger.info(
                    f"Generating tasks for upload {upload_id} due to overall time threshold: "
                    f"(threshold: {probe_time_threshold} seconds)"
                )

            # Update upload status to active if it's pending
            upload_info = await db.get_upload_request(upload_id)
            if upload_info and upload_info["status"] == "pending":
                await update_upload_request_status(upload_id, "active")
                logger.info(f"Updated upload {upload_id} status to active")

            # Create file tasks for each file
            processed_files = set()
            for file_path in all_changed_files:
                try:
                    # Only process the file if it exists
                    if os.path.exists(file_path):
                        await create_file_task(upload_id, file_path, source_folder)
                        processed_files.add(file_path)
                except Exception as e:
                    logger.error(f"Error creating file task for {file_path}: {e}")

            logger.info(f"Created {len(processed_files)} file tasks for upload {upload_id}")

        else:
            logger.debug(
                f"Changes for upload {upload_id} have not reached thresholds: "
                f"Size: {total_change_size_mb:.2f} MB / {probe_chunk_size_mb} MB, "
                f"No individual files exceed age threshold"
            )


# Create global monitor instance
monitor = FileMonitor()
