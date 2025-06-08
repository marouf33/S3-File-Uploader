"""
Database operations for the S3 File Upload Service.
Uses aiosqlite for async database operations.
"""

import logging
import os
import sqlite3
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any

import aiosqlite

from app.config import config
from app.models import UploadRequest, UploadStatus

logger = logging.getLogger(__name__)

# Database schema definitions
SCHEMA = [
    """
    CREATE TABLE IF NOT EXISTS upload_requests (
        upload_id TEXT PRIMARY KEY,
        source_folder TEXT NOT NULL,
        destination_bucket TEXT NOT NULL,
        pattern TEXT NOT NULL,
        status TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS file_tasks (
        task_id TEXT PRIMARY KEY,
        upload_id TEXT NOT NULL,
        file_path TEXT NOT NULL,
        relative_path TEXT NOT NULL,
        size INTEGER NOT NULL,
        last_modified TEXT NOT NULL,
        status TEXT NOT NULL,
        uploaded_bytes INTEGER NOT NULL,
        error TEXT,
        multipart_upload_id TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        FOREIGN KEY (upload_id) REFERENCES upload_requests (upload_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS chunk_tracking (
        chunk_id TEXT PRIMARY KEY,
        task_id TEXT NOT NULL,
        chunk_number INTEGER NOT NULL,
        start_byte INTEGER NOT NULL,
        end_byte INTEGER NOT NULL,
        etag TEXT,
        status TEXT NOT NULL,
        uploaded_at TEXT,
        FOREIGN KEY (task_id) REFERENCES file_tasks (task_id)
    )
    """,
]


class Database:
    """Database manager for async SQLite operations."""

    def __init__(self, db_path: str = None):
        """Initialize database connection."""
        self.db_path = db_path or config.db_path
        self.connection = None

    async def connect(self) -> None:
        """Establish connection to the database."""
        logger.info(f"Connecting to database at {self.db_path}")
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(os.path.abspath(self.db_path)), exist_ok=True)

            # Connect to the database
            self.connection = await aiosqlite.connect(self.db_path)

            # Enable foreign keys
            await self.connection.execute("PRAGMA foreign_keys = ON")

            # Initialize database schema
            await self._initialize_schema()

            logger.info("Database connection established")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    async def disconnect(self) -> None:
        """Close the database connection."""
        if self.connection:
            await self.connection.close()
            self.connection = None
            logger.info("Database connection closed")

    async def _initialize_schema(self) -> None:
        """Initialize database schema if it doesn't exist."""
        async with self.connection.cursor() as cursor:
            for statement in SCHEMA:
                await cursor.execute(statement)
            await self.connection.commit()

    async def save_upload_request(self, upload_request: UploadRequest, status: str = "pending") -> str:
        """
        Save an upload request to the database.

        Args:
            upload_request: The upload request to save
            status: The initial status of the upload request (default: "pending")

        Returns:
            The upload_id of the saved request
        """
        now = datetime.now(timezone.utc).isoformat()

        try:
            async with self.connection.cursor() as cursor:
                await cursor.execute(
                    """
                    INSERT INTO upload_requests (
                        upload_id, source_folder, destination_bucket, pattern,
                        status, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        upload_request.upload_id,
                        upload_request.source_folder,
                        upload_request.destination_bucket,
                        upload_request.pattern,
                        status,
                        now,
                        now,
                    ),
                )
                await self.connection.commit()

            logger.info(f"Saved upload request with ID: {upload_request.upload_id}")
            return upload_request.upload_id
        except sqlite3.IntegrityError:
            logger.error(f"Upload request with ID {upload_request.upload_id} already exists")
            raise ValueError(f"Upload request with ID {upload_request.upload_id} already exists")
        except Exception as e:
            logger.error(f"Failed to save upload request: {e}")
            raise

    async def get_upload_request(self, upload_id: str) -> Optional[Dict[str, Any]]:
        """
        Get an upload request by ID.

        Args:
            upload_id: The ID of the upload request to retrieve

        Returns:
            The upload request as a dictionary, or None if not found
        """
        try:
            async with self.connection.cursor() as cursor:
                await cursor.execute("SELECT * FROM upload_requests WHERE upload_id = ?", (upload_id,))
                row = await cursor.fetchone()

                if not row:
                    return None

                # Convert row to dictionary
                columns = [column[0] for column in cursor.description]
                return {columns[i]: row[i] for i in range(len(columns))}
        except Exception as e:
            logger.error(f"Failed to get upload request {upload_id}: {e}")
            raise

    async def get_all_upload_requests(self) -> List[Dict[str, Any]]:
        """
        Get all upload requests.

        Returns:
            A list of all upload requests as dictionaries
        """
        try:
            async with self.connection.cursor() as cursor:
                await cursor.execute("SELECT * FROM upload_requests")
                rows = await cursor.fetchall()

                # Convert rows to dictionaries
                columns = [column[0] for column in cursor.description]
                return [{columns[i]: row[i] for i in range(len(columns))} for row in rows]
        except Exception as e:
            logger.error(f"Failed to get all upload requests: {e}")
            raise

    async def get_upload_status(self, upload_id: str) -> Optional[UploadStatus]:
        """
        Get the status of an upload request.

        Args:
            upload_id: The ID of the upload request

        Returns:
            An UploadStatus object, or None if not found
        """
        try:
            # Get the upload request
            upload_request = await self.get_upload_request(upload_id)
            if not upload_request:
                return None

            # Get file task statistics
            async with self.connection.cursor() as cursor:
                await cursor.execute(
                    """
                    SELECT
                        COUNT(*) as files_total,
                        SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as files_uploaded,
                        SUM(size) as bytes_total,
                        SUM(uploaded_bytes) as bytes_uploaded
                    FROM file_tasks
                    WHERE upload_id = ?
                    """,
                    (upload_id,),
                )
                stats = await cursor.fetchone()

                # Create UploadStatus object
                return UploadStatus(
                    upload_id=upload_id,
                    status=upload_request['status'],
                    source_folder=upload_request['source_folder'],
                    destination_bucket=upload_request['destination_bucket'],
                    pattern=upload_request['pattern'],
                    files_total=stats[0] or 0,
                    files_uploaded=stats[1] or 0,
                    bytes_total=stats[2] or 0,
                    bytes_uploaded=stats[3] or 0,
                )
        except Exception as e:
            logger.error(f"Failed to get upload status for {upload_id}: {e}")
            raise

    async def update_upload_status(self, upload_id: str, status: str) -> bool:
        """
        Update the status of an upload request.

        Args:
            upload_id: The ID of the upload request
            status: The new status

        Returns:
            True if successful, False if the upload request was not found
        """
        now = datetime.now(timezone.utc).isoformat()

        try:
            async with self.connection.cursor() as cursor:
                await cursor.execute(
                    """
                    UPDATE upload_requests
                    SET status = ?, updated_at = ?
                    WHERE upload_id = ?
                    """,
                    (status, now, upload_id),
                )
                await self.connection.commit()

                return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Failed to update status for upload {upload_id}: {e}")
            raise

    async def delete_upload_request(self, upload_id: str) -> bool:
        """
        Delete an upload request and its associated tasks.

        Args:
            upload_id: The ID of the upload request to delete

        Returns:
            True if successful, False if the upload request was not found
        """
        try:
            async with self.connection.cursor() as cursor:
                # Delete associated chunks first (due to foreign key constraints)
                await cursor.execute(
                    """
                    DELETE FROM chunk_tracking
                    WHERE task_id IN (
                        SELECT task_id FROM file_tasks WHERE upload_id = ?
                    )
                    """,
                    (upload_id,),
                )

                # Delete file tasks
                await cursor.execute("DELETE FROM file_tasks WHERE upload_id = ?", (upload_id,))

                # Delete upload request
                await cursor.execute("DELETE FROM upload_requests WHERE upload_id = ?", (upload_id,))

                await self.connection.commit()

                return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Failed to delete upload request {upload_id}: {e}")
            raise


# Create global database instance
db = Database()
