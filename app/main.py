"""
Main FastAPI application for S3 File Upload Service.
"""

import logging
import uuid
from typing import List
from contextlib import asynccontextmanager
import asyncio

from fastapi import FastAPI, HTTPException, status

# Import models from models.py
from app.models import UploadRequest, UploadResponse, UploadStatus
from app.db import db
from app.monitor import monitor
from app.s3 import uploader
from app.config import config
from app.tasks import requeue_failed_tasks, reset_in_progress_tasks

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Set debug level for monitor module to see file change logs
logging.getLogger("app.monitor").setLevel(logging.DEBUG)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager for application startup and shutdown events.
    This replaces the deprecated @app.on_event("startup"/"shutdown") approach.
    """
    # Startup: initialize resources
    logger.info("Starting S3 File Upload Service")

    # Initialize database connection
    await db.connect()

    # Reset any tasks that were in progress when the application crashed
    reset_count = await reset_in_progress_tasks()
    if reset_count > 0:
        logger.warning(f"Found and reset {reset_count} tasks that were interrupted by previous crash")

    # Start file monitoring service
    await monitor.start()

    await uploader.start()

    # Start background task for requeuing failed tasks
    asyncio.create_task(failed_task_requeue_worker())

    yield  # Application execution happens here

    # Shutdown: clean up resources
    logger.info("Shutting down S3 File Upload Service")

    await uploader.stop()

    # Stop file monitoring service
    await monitor.stop()

    # Close database connection
    await db.disconnect()


# Create FastAPI app with lifespan
app = FastAPI(
    title="S3 File Upload Service",
    description="Service for monitoring and uploading files to S3",
    version="0.1.0",
    lifespan=lifespan,
)


async def failed_task_requeue_worker():
    while True:
        try:
            await requeue_failed_tasks(config.failed_task_requeue_interval)
        except Exception as e:
            logger.error(f"Failed task requeue worker error: {e}")
        await asyncio.sleep(config.failed_task_requeue_interval)


@app.get("/")
async def root():
    """Root endpoint."""
    return {"message": "S3 File Upload Service"}


@app.post("/uploads", response_model=UploadResponse, status_code=status.HTTP_201_CREATED)
async def create_upload(upload_request: UploadRequest):
    """Create a new upload request."""
    # Generate UUID if not provided
    if not upload_request.upload_id:
        upload_request.upload_id = str(uuid.uuid4())

    logger.info(f"Received upload request: {upload_request.dict()}")

    try:
        # Save the upload request to the database
        await db.save_upload_request(upload_request)

        return UploadResponse(
            upload_id=upload_request.upload_id, message=f"Upload request accepted for {upload_request.source_folder}"
        )
    except ValueError as e:
        # Handle duplicate upload_id
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error creating upload request: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/uploads", response_model=List[UploadStatus])
async def list_uploads():
    """List all active upload requests."""
    logger.info("Listing all uploads")

    try:
        # Get all upload requests from the database
        upload_requests = await db.get_all_upload_requests()

        # Convert to UploadStatus objects
        result = []
        for req in upload_requests:
            status = await db.get_upload_status(req["upload_id"])
            if status:
                result.append(status)

        return result
    except Exception as e:
        logger.error(f"Error listing upload requests: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/uploads/{upload_id}", response_model=UploadStatus)
async def get_upload_status(upload_id: str):
    """Get status of a specific upload request."""
    logger.info(f"Getting status for upload: {upload_id}")

    try:
        # Retrieve upload status from the database
        status = await db.get_upload_status(upload_id)
        if not status:
            raise HTTPException(status_code=404, detail="Upload not found")
        return status
    except Exception as e:
        logger.error(f"Error retrieving upload status: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

    # TODO: Implement retrieval of upload status


@app.delete("/uploads/{upload_id}", status_code=status.HTTP_202_ACCEPTED)
async def cancel_upload(upload_id: str):
    """
    Cancel an upload request and delete all associated file tasks and chunks.

    This endpoint will:
    1. Check if the upload exists
    2. Delete all chunks associated with the upload's file tasks
    3. Delete all file tasks associated with the upload
    4. Delete the upload request itself

    Args:
        upload_id: The ID of the upload request to cancel

    Returns:
        A message confirming successful cancellation

    Raises:
        404: If the upload is not found
        500: If there's an error during the cancellation process
    """
    logger.info(f"Cancelling upload: {upload_id}")
    try:
        # Check if the upload exists
        status = await db.get_upload_status(upload_id)
        if not status:
            raise HTTPException(status_code=404, detail="Upload not found")

        # Delete the upload request and all associated file tasks and chunks
        deleted = await db.delete_upload_request(upload_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="Upload not found or already deleted")

        return {"message": f"Upload {upload_id} cancelled successfully with all associated file tasks"}
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        logger.error(f"Error cancelling upload: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/uploads/{upload_id}/tasks")
async def get_upload_tasks(upload_id: str):
    """Get all file tasks for an upload request."""
    logger.info(f"Getting tasks for upload: {upload_id}")

    try:
        # Check if the upload exists
        upload = await db.get_upload_request(upload_id)
        if not upload:
            raise HTTPException(status_code=404, detail="Upload not found")

        # Get all file tasks for this upload
        from app.tasks import get_file_tasks

        tasks = await get_file_tasks(upload_id)

        return {"upload_id": upload_id, "tasks": tasks}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting upload tasks: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
