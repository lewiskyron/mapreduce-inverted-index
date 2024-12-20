import os

# Server Configuration
MASTER_URL_DEFAULT = "http://master-service:5001"
REDUCER_URL_DEFAULT = "http://reducer-service:5003"
REDUCER_HOST = "0.0.0.0"
REDUCER_PORT = 5003

# Paths
BASE_DIR = "/app"
DATA_DIR = os.path.join(BASE_DIR, "data")
OUTPUT_DIR_DEFAULT = os.path.join(DATA_DIR, "final")

# Registration Configuration
MAX_RETRIES = 5
RETRY_DELAY = 3  # seconds

# Task States
TASK_STATES = {
    "IDLE": "idle",
    "IN_PROGRESS": "in_progress",
    "COMPLETED": "completed",
    "FAILED": "failed",
}

# Response Status
STATUS = {"SUCCESS": "success", "ERROR": "error"}

# API Messages
MESSAGES = {
    "MISSING_PARAMS": "Missing required parameters",
    "PROCESSING_STARTED": "Reduce task processing started",
    "MAPPER_DATA_REQUIRED": "mapper_url and result_location required",
}
