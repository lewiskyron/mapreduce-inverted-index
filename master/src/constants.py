# URLs and Endpoints
WIKIPEDIA_BASE_URL = "https://en.wikipedia.org"
CATEGORY_URL = "https://en.wikipedia.org/wiki/Category:Rock_musical_group_stubs"
HEARTBEAT_INTERVAL = 5
MAX_RETRIES = 3  # max retries for failed tasks
WORKER_TIMEOUT = 10 # seconds before worker considered dead

# Server Configuration
MASTER_HOST = "0.0.0.0"
MASTER_PORT = 5001

# MapReduce Configuration
DEFAULT_CHUNK_SIZE = 200
MAPPER_TIMEOUT = 60 # seconds

# Logging Configuration
LOG_FORMAT = "%(asctime)s %(levelname)s:%(message)s"
LOG_LEVEL = "INFO"

# API Response Messages
MESSAGES = {
    "SCRAPING_IN_PROGRESS": "Scraping already in progress",
    "NO_MAPPERS": "No mapper URLs configured",
    "NO_URLS": "No URLs collected",
}

# API Status Codes
STATUS = {
    "SUCCESS": "success",
    "ERROR": "error",
}

# Environment Variables
ENV_MAPPER_URLS = "MAPPER_URLS"

TASK_STATES = {
    "IDLE": "idle",
    "IN_PROGRESS": "in_progress",
    "COMPLETED": "completed",
    "FAILED": "failed",
}
