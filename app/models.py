"""
Pydantic models for the S3 File Upload Service.
"""

from typing import List, Optional

from pydantic import BaseModel, Field


class UploadRequest(BaseModel):
    """Model for upload request."""

    upload_id: Optional[str] = Field(
        default=None,
        description="Optional identifier for the upload request. If not provided, a UUID will be generated.",
    )
    source_folder: str = Field(..., description="Source folder to monitor for files")
    destination_bucket: str = Field(..., description="The bucket name in the destination storage provider")
    pattern: Optional[str] = Field(default="*", description="Optional glob pattern to filter files to be uploaded")


class UploadResponse(BaseModel):
    """Model for upload response."""

    upload_id: str
    status: str = "pending"
    message: str


class UploadStatus(BaseModel):
    """Model for upload status."""

    upload_id: str
    status: str
    source_folder: str
    destination_bucket: str
    pattern: str
    files_total: int = 0
    files_uploaded: int = 0
    bytes_total: int = 0
    bytes_uploaded: int = 0
