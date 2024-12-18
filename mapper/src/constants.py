# mapper/src/constants.py
import os

# Paths
BASE_DIR = "/app"
DATA_DIR = os.path.join(BASE_DIR, "data")
INTERMEDIATE_DIR = os.path.join(DATA_DIR, "intermediate")

# File naming
INTERMEDIATE_FILE_FORMAT = "mapper_{mapper_id}_part_{part_id}.json"
