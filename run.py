"""
Run the S3 File Upload Service application.
"""
import argparse
import uvicorn
from app.config import config

if __name__ == "__main__":
    # Setup command line arguments
    parser = argparse.ArgumentParser(description="Run the S3 File Upload Service")
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug level logging"
    )
    args = parser.parse_args()

    # Run the FastAPI application using uvicorn
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=config.application_port,
        reload=False,  # Enable auto-reload during development
        log_level="debug" if args.debug else "info",
    )
