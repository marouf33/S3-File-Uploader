"""
Configuration settings for the S3 File Upload Service.
"""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppConfig(BaseSettings):
    """Application configuration settings."""

    application_port: int = Field(default=8000, description="Port on which the application will run")

    # AWS Configuration
    aws_access_key_id: str = Field(..., description="AWS Access Key ID for S3 access")
    aws_secret_access_key: str = Field(..., description="AWS Secret Access Key for S3 access")
    aws_region: str = Field(default="us-east-1", description="AWS Region for S3 buckets")

    # Monitoring Configuration
    probe_interval: int = Field(default=10, description="Interval in seconds between directory scans")
    probe_chunk_size_mb: int = Field(
        default=5, description="Minimum accumulated size in MB before generating tasks"  # 5 MB
    )
    probe_time_threshold: int = Field(
        default=300,  # 5 minutes
        description="Maximum time in seconds to wait before generating tasks regardless of size",
    )
    upload_check_interval: int = Field(default=10, description="Interval in seconds between upload task checks")

    # Database Configuration
    db_path: str = Field(default="upload_service.db", description="Path to SQLite database file")

    # Service Configuration
    max_concurrent_uploads: int = Field(default=5, description="Maximum number of concurrent uploads")
    upload_retry_attempts: int = Field(default=3, description="Number of retry attempts for failed uploads")
    failed_task_requeue_interval: int = Field(
        default=3600, description="Time in seconds before a failed upload task is requeued for retry"  # 1 hour
    )

    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        case_sensitive=False,  # Ignore extra fields not defined in the model
    )


# Create global config instance
config = AppConfig()
