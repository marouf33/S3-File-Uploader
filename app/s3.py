"""
S3 file upload functionality for the S3 File Upload Service.

This module provides asynchronous uploading of files to S3 buckets,
including support for multipart uploads, chunking, and error handling.
The implementation uses configuration values from AppConfig for various
settings such as chunk size, concurrent uploads, and retry attempts.
"""

import asyncio
import logging
import os

import aioboto3
from botocore.exceptions import ClientError

from app.config import config
from app.db import db
from app.tasks import (
    get_pending_file_tasks,
    update_task_status,
    update_task_progress,
    get_multipart_upload_id,
    set_multipart_upload_id,
    get_chunks,
    create_or_update_chunk,
    update_chunk_status,
    check_upload_completion,
)

logger = logging.getLogger(__name__)


class S3Uploader:
    """
    Asynchronous S3 file uploader.

    Handles uploading files to S3 buckets using aioboto3, with support for:
    - Multipart uploads for large files
    - Chunk tracking for resumable uploads
    - Error handling and retries
    """

    def __init__(self, chunk_size: int = None, max_concurrent_uploads: int = None, retry_attempts: int = None):
        """
        Initialize the S3 uploader.

        Args:
            chunk_size: Size of chunks for multipart uploads in bytes (defaults to AppConfig value)
            max_concurrent_uploads: Maximum number of concurrent uploads (defaults to AppConfig value)
            retry_attempts: Number of retry attempts for failed uploads (defaults to AppConfig value)
        """
        # Use probe_chunk_size_mb from config, converting from MB to bytes
        self.chunk_size = chunk_size or (config.probe_chunk_size_mb * 1024 * 1024)
        self.max_concurrent_uploads = max_concurrent_uploads or config.max_concurrent_uploads
        self.retry_attempts = retry_attempts or config.upload_retry_attempts

        # Create AWS session using credentials from config
        self.session = aioboto3.Session(
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key,
            region_name=config.aws_region,
        )
        self._upload_semaphore = asyncio.Semaphore(self.max_concurrent_uploads)
        self._running = False
        self._task = None

    async def start(self):
        """Start the S3 upload worker."""
        if self._running:
            logger.warning("S3 uploader already running")
            return

        self._running = True
        self._task = asyncio.create_task(self._upload_worker())
        logger.info("S3 upload worker started")

    async def stop(self):
        """Stop the S3 upload worker."""
        if not self._running:
            logger.warning("S3 uploader not running")
            return

        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        logger.info("S3 upload worker stopped")

    async def _upload_worker(self):
        """Main worker loop that processes pending upload tasks."""
        while self._running:
            try:
                # Process a batch of pending uploads
                await self._process_pending_uploads()
            except Exception as e:
                logger.error(f"Error in upload worker: {e}")

            # Wait before checking for more uploads using config value
            await asyncio.sleep(config.upload_check_interval)

    async def _process_pending_uploads(self):
        """Process pending upload tasks from the database."""
        # Get pending file tasks using the tasks module function
        tasks = await get_pending_file_tasks(self.max_concurrent_uploads * 2)

        if not tasks:
            return

        logger.info(f"Found {len(tasks)} pending upload tasks (max concurrent: {self.max_concurrent_uploads})")

        # Process each task
        upload_tasks = []
        for task in tasks:
            # Create a task for each upload but limit concurrency with the semaphore
            upload_tasks.append(asyncio.create_task(self._process_upload_task(task)))

        # Wait for all uploads to complete
        if upload_tasks:
            await asyncio.gather(*upload_tasks, return_exceptions=True)

    async def _process_upload_task(self, task):
        """
        Process a single upload task.

        Args:
            task: The file task to process
        """
        task_id = task["task_id"]
        file_path = task["file_path"]
        upload_id = task["upload_id"]

        # Acquire semaphore to limit concurrent uploads
        async with self._upload_semaphore:
            logger.info(f"Processing upload task {task_id} for file {file_path}")

            try:
                # Update task status to 'in_progress'
                await update_task_status(task_id, "in_progress")

                # Get the upload request to determine the destination bucket
                upload_request = await db.get_upload_request(upload_id)
                if not upload_request:
                    logger.error(f"Upload request {upload_id} not found for task {task_id}")
                    await update_task_status(task_id, "failed", "Upload request not found")
                    return

                destination_bucket = upload_request["destination_bucket"]
                relative_path = task["relative_path"]

                # Determine S3 key (path in the bucket)
                monitored_dir_name = os.path.basename(upload_request["source_folder"].rstrip(os.sep))
                s3_key = f"{monitored_dir_name}/{relative_path.replace('\\', '/')}"  # Ensure forward slashes for S3

                # Check if file exists
                if not os.path.exists(file_path):
                    logger.error(f"File {file_path} not found for task {task_id}")
                    await update_task_status(task_id, "failed", "File not found")
                    return

                # Get file size
                file_size = os.path.getsize(file_path)

                # Determine upload method based on file size
                if file_size > self.chunk_size:
                    # Use multipart upload for large files (chunk size from config)
                    success = await self._upload_multipart(task_id, file_path, destination_bucket, s3_key, file_size)
                else:
                    # Use simple upload for small files
                    success = await self._upload_simple(task_id, file_path, destination_bucket, s3_key, file_size)

                if success:
                    # Update task status to 'completed'
                    await update_task_status(task_id, "completed", uploaded_bytes=file_size)

                    # Check if all tasks for this upload are complete
                    await check_upload_completion(upload_id)
                else:
                    # Update task status to 'failed'
                    await update_task_status(task_id, "failed", "Upload failed")
            except Exception as e:
                logger.error(f"Error processing upload task {task_id}: {e}")
                await update_task_status(task_id, "failed", str(e))

    async def _upload_simple(self, task_id, file_path, bucket, key, file_size):
        """
        Upload a small file to S3 in a single request.

        Args:
            task_id: The ID of the file task
            file_path: The path to the file
            bucket: The S3 bucket name
            key: The S3 object key
            file_size: The size of the file in bytes

        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Starting simple upload for {file_path} to {bucket}/{key}")

        # Use retry_attempts from config
        for attempt in range(1, self.retry_attempts + 1):
            try:
                async with self.session.client('s3') as s3:
                    with open(file_path, 'rb') as file_obj:
                        await s3.upload_fileobj(
                            file_obj,
                            bucket,
                            key,
                            Callback=lambda bytes_transferred: asyncio.create_task(
                                update_task_progress(task_id, bytes_transferred)
                            ),
                        )

                logger.info(f"Simple upload completed for {file_path}")
                return True
            except Exception as e:
                logger.error(f"Error in simple upload (attempt {attempt}/{self.retry_attempts}): {e}")
                if attempt < self.retry_attempts:
                    # Exponential backoff: wait 2^attempt seconds before retrying
                    await asyncio.sleep(2**attempt)
                else:
                    return False

    async def _upload_multipart(self, task_id, file_path, bucket, key, file_size):
        """
        Upload a large file to S3 using multipart upload.

        Args:
            task_id: The ID of the file task
            file_path: The path to the file
            bucket: The S3 bucket name
            key: The S3 object key
            file_size: The size of the file in bytes

        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Starting multipart upload for {file_path} to {bucket}/{key}")

        # Check for existing chunks in the database
        chunks = await get_chunks(task_id)
        completed_chunks = {chunk["chunk_number"]: chunk for chunk in chunks if chunk["status"] == "completed"}

        # Calculate the total number of chunks
        total_chunks = (file_size + self.chunk_size - 1) // self.chunk_size

        try:
            async with self.session.client('s3') as s3:
                # Start a new multipart upload or get existing one
                multipart_upload_id = await self._get_or_create_multipart_upload(s3, task_id, bucket, key)

                if not multipart_upload_id:
                    logger.error(f"Failed to create multipart upload for {file_path}")
                    return False

                # Upload each chunk
                parts = []

                for chunk_number in range(1, total_chunks + 1):
                    # Skip if chunk already completed
                    if chunk_number in completed_chunks:
                        logger.info(f"Chunk {chunk_number}/{total_chunks} already completed for {file_path}")
                        parts.append({'PartNumber': chunk_number, 'ETag': completed_chunks[chunk_number]["etag"]})
                        continue

                    # Calculate byte range for this chunk
                    start_byte = (chunk_number - 1) * self.chunk_size
                    end_byte = min(start_byte + self.chunk_size, file_size) - 1

                    # Create or update chunk in database
                    chunk_id = await create_or_update_chunk(task_id, chunk_number, start_byte, end_byte)

                    # Upload the chunk
                    etag = await self._upload_chunk(
                        s3, file_path, bucket, key, multipart_upload_id, chunk_number, start_byte, end_byte
                    )

                    if not etag:
                        logger.error(f"Failed to upload chunk {chunk_number} for {file_path}")
                        return False

                    # Update chunk status in database
                    await update_chunk_status(chunk_id, "completed", etag)

                    # Add to parts list for completing the multipart upload
                    parts.append({'PartNumber': chunk_number, 'ETag': etag})

                    # Update overall progress
                    bytes_uploaded = min((chunk_number * self.chunk_size), file_size)
                    await update_task_progress(task_id, bytes_uploaded)

                # Complete the multipart upload
                await s3.complete_multipart_upload(
                    Bucket=bucket, Key=key, UploadId=multipart_upload_id, MultipartUpload={'Parts': parts}
                )

                logger.info(f"Multipart upload completed for {file_path}")
                return True

        except Exception as e:
            logger.error(f"Error in multipart upload: {e}")
            return False

    async def _get_or_create_multipart_upload(self, s3_client, task_id, bucket, key):
        """
        Get an existing multipart upload ID or create a new one.

        Args:
            s3_client: The S3 client
            task_id: The file task ID
            bucket: The S3 bucket
            key: The S3 object key

        Returns:
            The multipart upload ID
        """
        # Check if we have a saved multipart upload ID in the database
        multipart_upload_id = await get_multipart_upload_id(task_id)

        if multipart_upload_id:
            # Validate that the multipart upload still exists in S3
            try:
                await s3_client.list_parts(Bucket=bucket, Key=key, UploadId=multipart_upload_id)
                return multipart_upload_id
            except ClientError:
                # Multipart upload no longer exists in S3, create a new one
                pass

        # Create a new multipart upload
        try:
            response = await s3_client.create_multipart_upload(Bucket=bucket, Key=key)
            multipart_upload_id = response['UploadId']

            # Save to database using the tasks module function
            await set_multipart_upload_id(task_id, multipart_upload_id)

            return multipart_upload_id
        except Exception as e:
            logger.error(f"Error creating multipart upload: {e}")
            return None

    async def _upload_chunk(self, s3_client, file_path, bucket, key, upload_id, chunk_number, start_byte, end_byte):
        """
        Upload a single chunk of a file to S3.

        Args:
            s3_client: The S3 client
            file_path: Path to the file
            bucket: The S3 bucket
            key: The S3 object key
            upload_id: The multipart upload ID
            chunk_number: The part number
            start_byte: The starting byte position
            end_byte: The ending byte position

        Returns:
            The ETag of the uploaded part if successful, None otherwise
        """
        chunk_size = end_byte - start_byte + 1

        # Use retry_attempts from config
        for attempt in range(1, self.retry_attempts + 1):
            try:
                with open(file_path, 'rb') as f:
                    f.seek(start_byte)
                    chunk_data = f.read(chunk_size)

                response = await s3_client.upload_part(
                    Bucket=bucket,
                    Key=key,
                    PartNumber=chunk_number,
                    UploadId=upload_id,
                    Body=chunk_data,
                    ContentLength=chunk_size,
                )

                return response['ETag']
            except Exception as e:
                logger.error(f"Error uploading chunk {chunk_number} (attempt {attempt}/{self.retry_attempts}): {e}")
                if attempt < self.retry_attempts:
                    # Exponential backoff
                    await asyncio.sleep(2**attempt)
                else:
                    return None


# Create global uploader instance with config values
uploader = S3Uploader(
    chunk_size=config.probe_chunk_size_mb * 1024 * 1024,
    max_concurrent_uploads=config.max_concurrent_uploads,
    retry_attempts=config.upload_retry_attempts,
)
