"""
Methods for creating and managing file upload tasks in the database.
"""

import logging
import os
import uuid
import hashlib
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any

from app.db import db

logger = logging.getLogger(__name__)


async def create_file_task(upload_id: str, file_path: str, source_folder: str) -> str:
    """
    Create a new file task for an upload request.

    Args:
        upload_id: The ID of the upload request
        file_path: The absolute path to the file
        source_folder: The source folder of the upload request

    Returns:
        The task_id of the created file task
    """
    # Get file information
    file_size = os.path.getsize(file_path)
    last_modified = datetime.fromtimestamp(os.path.getmtime(file_path), tz=timezone.utc).isoformat()

    # Calculate relative path
    relative_path = os.path.relpath(file_path, source_folder)

    # Generate task ID
    task_id = str(uuid.uuid4())

    # Current time
    now = datetime.now(timezone.utc).isoformat()

    # Insert file task into database
    async with db.connection.cursor() as cursor:
        await cursor.execute(
            """
            INSERT INTO file_tasks (
                task_id, upload_id, file_path, relative_path,
                size, last_modified, status, uploaded_bytes, error,
                multipart_upload_id, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (task_id, upload_id, file_path, relative_path, file_size, last_modified, "pending", 0, None, None, now, now),
        )
        await db.connection.commit()

    logger.info(f"Created file task {task_id} for file {file_path}")
    return task_id


async def update_upload_request_status(upload_id: str, status: str) -> bool:
    """
    Update the status of an upload request.

    Args:
        upload_id: The ID of the upload request
        status: The new status

    Returns:
        True if successful, False if the upload request was not found
    """
    return await db.update_upload_status(upload_id, status)


async def get_file_tasks(upload_id: str) -> List[Dict]:
    """
    Get all file tasks for an upload request.

    Args:
        upload_id: The ID of the upload request

    Returns:
        A list of file tasks as dictionaries
    """
    tasks = []
    async with db.connection.cursor() as cursor:
        await cursor.execute("SELECT * FROM file_tasks WHERE upload_id = ?", (upload_id,))
        rows = await cursor.fetchall()

        # Convert rows to dictionaries
        columns = [column[0] for column in cursor.description]
        tasks = [{columns[i]: row[i] for i in range(len(columns))} for row in rows]

    return tasks


async def get_pending_file_tasks(max_tasks: int) -> List[Dict[str, Any]]:
    """
    Get pending file tasks from all active uploads.

    Args:
        max_tasks: Maximum number of tasks to retrieve

    Returns:
        A list of pending file tasks
    """
    async with db.connection.cursor() as cursor:
        await cursor.execute(
            """
            SELECT ft.* FROM file_tasks ft
            JOIN upload_requests ur ON ft.upload_id = ur.upload_id
            WHERE ft.status = 'pending' AND ur.status = 'active'
            ORDER BY ft.created_at ASC
            LIMIT ?
            """,
            (max_tasks,),
        )
        tasks = await cursor.fetchall()

        if not tasks:
            return []

        # Convert rows to dictionaries
        columns = [column[0] for column in cursor.description]
        return [{columns[i]: row[i] for i in range(len(columns))} for row in tasks]


async def update_task_status(task_id: str, status: str, error: str = None, uploaded_bytes: int = None) -> None:
    """
    Update the status of a file task.

    Args:
        task_id: The task ID
        status: The new status
        error: Optional error message
        uploaded_bytes: Optional number of bytes uploaded
    """
    now = datetime.now(timezone.utc).isoformat()

    async with db.connection.cursor() as cursor:
        if uploaded_bytes is not None:
            await cursor.execute(
                """
                UPDATE file_tasks
                SET status = ?, error = ?, uploaded_bytes = ?, updated_at = ?
                WHERE task_id = ?
                """,
                (status, error, uploaded_bytes, now, task_id),
            )
        else:
            await cursor.execute(
                """
                UPDATE file_tasks
                SET status = ?, error = ?, updated_at = ?
                WHERE task_id = ?
                """,
                (status, error, now, task_id),
            )

        await db.connection.commit()


async def update_task_progress(task_id: str, uploaded_bytes: int) -> None:
    """
    Update the progress of a file task.

    Args:
        task_id: The task ID
        uploaded_bytes: The number of bytes uploaded
    """
    now = datetime.now(timezone.utc).isoformat()

    async with db.connection.cursor() as cursor:
        await cursor.execute(
            """
            UPDATE file_tasks
            SET uploaded_bytes = ?, updated_at = ?
            WHERE task_id = ? AND uploaded_bytes < ?
            """,
            (uploaded_bytes, now, task_id, uploaded_bytes),
        )
        await db.connection.commit()


async def get_multipart_upload_id(task_id: str) -> Optional[str]:
    """
    Get the multipart upload ID for a file task.

    Args:
        task_id: The file task ID

    Returns:
        The multipart upload ID, or None if not found
    """
    async with db.connection.cursor() as cursor:
        await cursor.execute("SELECT multipart_upload_id FROM file_tasks WHERE task_id = ?", (task_id,))
        result = await cursor.fetchone()

        if result and result[0]:
            return result[0]
        return None


async def set_multipart_upload_id(task_id: str, multipart_upload_id: str) -> None:
    """
    Set the multipart upload ID for a file task.

    Args:
        task_id: The file task ID
        multipart_upload_id: The multipart upload ID
    """
    async with db.connection.cursor() as cursor:
        await cursor.execute(
            "UPDATE file_tasks SET multipart_upload_id = ? WHERE task_id = ?", (multipart_upload_id, task_id)
        )
        await db.connection.commit()


async def get_chunks(task_id: str) -> List[Dict[str, Any]]:
    """
    Get all chunks for a file task.

    Args:
        task_id: The file task ID

    Returns:
        List of chunks
    """
    chunks = []
    async with db.connection.cursor() as cursor:
        await cursor.execute("SELECT * FROM chunk_tracking WHERE task_id = ? ORDER BY chunk_number", (task_id,))
        rows = await cursor.fetchall()

        # Convert rows to dictionaries
        columns = [column[0] for column in cursor.description]
        chunks = [{columns[i]: row[i] for i in range(len(columns))} for row in rows]

    return chunks


async def create_or_update_chunk(task_id: str, chunk_number: int, start_byte: int, end_byte: int) -> str:
    """
    Create a new chunk record or update an existing one.

    Args:
        task_id: The file task ID
        chunk_number: The chunk number
        start_byte: The starting byte position
        end_byte: The ending byte position

    Returns:
        The chunk ID
    """
    # Check if chunk already exists
    async with db.connection.cursor() as cursor:
        await cursor.execute(
            "SELECT chunk_id FROM chunk_tracking WHERE task_id = ? AND chunk_number = ?", (task_id, chunk_number)
        )
        result = await cursor.fetchone()

        if result:
            # Update existing chunk
            chunk_id = result[0]
            await cursor.execute(
                """
                UPDATE chunk_tracking
                SET start_byte = ?, end_byte = ?, status = 'pending'
                WHERE chunk_id = ?
                """,
                (start_byte, end_byte, chunk_id),
            )
        else:
            # Create new chunk
            chunk_id = str(hashlib.md5(f"{task_id}:{chunk_number}".encode()).hexdigest())
            await cursor.execute(
                """
                INSERT INTO chunk_tracking (
                    chunk_id, task_id, chunk_number, start_byte, end_byte, status
                ) VALUES (?, ?, ?, ?, ?, 'pending')
                """,
                (chunk_id, task_id, chunk_number, start_byte, end_byte),
            )

        await db.connection.commit()
        return chunk_id


async def update_chunk_status(chunk_id: str, status: str, etag: str = None) -> None:
    """
    Update the status of a chunk.

    Args:
        chunk_id: The chunk ID
        status: The new status
        etag: The ETag from S3 if completed
    """
    now = datetime.now(timezone.utc).isoformat()

    async with db.connection.cursor() as cursor:
        if etag:
            await cursor.execute(
                """
                UPDATE chunk_tracking
                SET status = ?, etag = ?, uploaded_at = ?
                WHERE chunk_id = ?
                """,
                (status, etag, now, chunk_id),
            )
        else:
            await cursor.execute("UPDATE chunk_tracking SET status = ? WHERE chunk_id = ?", (status, chunk_id))

        await db.connection.commit()


async def check_upload_completion(upload_id: str) -> bool:
    """
    Check if all tasks for an upload are complete.

    Args:
        upload_id: The upload ID

    Returns:
        True if all tasks are complete, False otherwise
    """
    async with db.connection.cursor() as cursor:
        # Count total and completed tasks
        await cursor.execute(
            """
            SELECT
                COUNT(*) as total_tasks,
                SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed_tasks
            FROM file_tasks
            WHERE upload_id = ?
            """,
            (upload_id,),
        )
        result = await cursor.fetchone()

        if result and result[0] > 0 and result[0] == result[1]:
            # All tasks are complete, update upload status to "active" to continue monitoring
            # but indicate all current tasks are completed
            await db.update_upload_status(upload_id, "active")
            logger.info(f"Upload {upload_id} current batch completed, continuing to monitor")
            return True
        return False


async def requeue_failed_tasks(min_failed_age_seconds: int):
    """
    Requeue failed tasks that have been failed for at least min_failed_age_seconds.
    Sets their status to 'pending' and clears the error field.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(seconds=min_failed_age_seconds)).isoformat()
    async with db.connection.cursor() as cursor:
        await cursor.execute(
            """
            UPDATE file_tasks
            SET status = 'pending', error = NULL, updated_at = ?
            WHERE status = 'failed' AND updated_at < ?
            """,
            (datetime.now(timezone.utc).isoformat(), cutoff),
        )
        await db.connection.commit()


async def reset_in_progress_tasks() -> int:
    """
    Reset tasks that were left in 'in_progress' state after a crash.
    Sets them to 'pending' status to be picked up by the upload worker.

    Returns:
        The number of tasks that were reset.
    """
    async with db.connection.cursor() as cursor:
        await cursor.execute(
            """
            UPDATE file_tasks
            SET status = 'pending', error = 'Application crashed during upload', updated_at = ?
            WHERE status = 'in_progress'
            """,
            (datetime.now(timezone.utc).isoformat(),),
        )

        # Get the number of affected rows
        count = cursor.rowcount
        await db.connection.commit()

    if count > 0:
        logger.info(f"Reset {count} tasks that were in progress during previous crash")

    return count
