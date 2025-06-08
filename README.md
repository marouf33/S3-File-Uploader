# S3 File Upload Service

A service that monitors specified directories for file changes and uploads matching files to S3 buckets.

## Features

- Upload files matching glob patterns to S3
- Maintain source folder structure in destination
- Monitor source folders for file changes
- Support chunked uploads for large files
- Provide fault tolerance and upload resumption
- Automatic requeuing of failed uploads after configurable time interval
- REST API for managing uploads

## Getting Started

### Prerequisites

- Python 3.12 or later
- AWS credentials with S3 access

### Installation

1. Clone the repository
2. Install the required packages:

```bash
pip install -r requirements.txt
```

3. Configure the application:

Edit the `.env` file and add your AWS credentials and other configuration options.

### Running the Application

Run the application using:

```bash
python run.py
```

This will start the FastAPI application with uvicorn on port 8000.

### Status Models

#### Upload Request Status
The service uses a simplified status model for upload requests:

- **pending**: Initial state when an upload request is created
- **active**: The upload is actively being processed and monitored for changes
- **cancelled**: Upload has been explicitly cancelled by the user

Directories start in the "pending" state when submitted and transition to "active" once the
monitor begins scanning them. They continue to be monitored as long as their status is "active".

#### File Task Status
Individual file tasks use a separate status model:

- **pending**: Task created but not yet started
- **in_progress**: File is currently being uploaded
- **completed**: File upload completed successfully
- **failed**: File upload failed with an error

This separation of concerns allows for clear tracking of both directory monitoring states and individual file operation states.

### Fault Tolerance Features

The service includes several features to ensure fault tolerance:

- **Automatic Failed Task Requeuing**: Failed upload tasks are automatically requeued after a configurable time interval (default: 1 hour)
- **Crash Recovery**: Tasks interrupted by application crashes are automatically detected and reset on startup
- **Continuous Monitoring**: Directories are continuously monitored for changes until explicitly cancelled, even after all current tasks complete
- **Task State Persistence**: All task states are stored in a SQLite database for recovery after service restarts
- **Chunked Uploads**: Large files are uploaded in chunks to enable resumption from the point of failure
- **Retry Logic**: Transient failures are retried with exponential backoff

### API Endpoints

- `GET /` - Service health check
- `POST /uploads` - Create a new upload request
- `GET /uploads` - List all active upload requests
- `GET /uploads/{upload_id}` - Get status of a specific upload request
- `DELETE /uploads/{upload_id}` - Cancel an upload request

## API Usage Examples

### Create an Upload Request

```bash
curl -X POST "http://localhost:8000/uploads" \
  -H "Content-Type: application/json" \
  -d '{
    "source_folder": "/path/to/source",
    "destination_bucket": "my-s3-bucket",
    "pattern": "*.txt"
  }'
```

### Check Upload Status

```bash
curl -X GET "http://localhost:8000/uploads/{upload_id}"
```

### Cancel an Upload

```bash
curl -X DELETE "http://localhost:8000/uploads/{upload_id}"
```

## Documentation

API documentation is available at:
- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc
