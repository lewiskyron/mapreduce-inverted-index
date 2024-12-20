import logging
from typing import List, Dict
import requests
from .constants import DEFAULT_CHUNK_SIZE, MAPPER_TIMEOUT, STATUS, TASK_STATES
from .map_functions import MAP_FUNCTIONS
from .task_monitor import TaskMonitor, TaskState
from datetime import datetime
import uuid
import time


class MapReduceCoordinator:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.mappers = {}  # Store mapper statuses
        self.task_monitor = TaskMonitor()
        self.task_monitor.start_monitoring()
        logging.info("MapReduceCoordinator initialized")

    def register_mapper(self, mapper_url: str):
        """Register a new mapper"""
        self.logger.info(f"Registering mapper: {mapper_url}")
        self.task_monitor.register_worker(mapper_url)
        return {"status": "registered", "mapper_url": mapper_url}

    def register_reducer(self, reducer_url: str):
        """Register a new reducer"""
        self.logger.info(f"Registering reducer: {reducer_url}")
        self.task_monitor.register_worker(reducer_url)
        return {"status": "registered", "reducer_url": reducer_url}

    def generate_task_id(self, mapper_url: str) -> str:
        """
        Generate a unique task ID using various components:
        - job_id: Incremental counter for each distribute_to_mappers call
        - timestamp: Current time in milliseconds
        - mapper_index: Index of the mapper in the current job
        - random: Random component for additional uniqueness
        """
        timestamp = int(time.time() * 1000)  # Current time in milliseconds
        mapper_id = mapper_url.split("/")[-1].replace(":", "_")
        random_component = uuid.uuid4().hex[:6]  # Short random string
        return f"task_{random_component}_{mapper_id}"

    def distribute_to_mappers(
        self,
        urls: List[str],
        mapper_urls: List[str],
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ) -> List[Dict]:
        """
        Distributes URLs to mappers with function references
        """
        results = []
        doc_id_start = 0

        for i, mapper_url in enumerate(mapper_urls):
            if mapper_url not in self.task_monitor.active_workers:
                self.register_mapper(mapper_url)
                self.logger.info(f"Registered new mapper: {mapper_url}")

            start_idx = i * chunk_size
            end_idx = start_idx + chunk_size
            url_chunk = urls[start_idx:end_idx]

            if not url_chunk:
                continue
            task_id = self.generate_task_id(mapper_url)

            task = TaskState(
                task_id=task_id,
                state=TASK_STATES["IDLE"],
                worker_url=mapper_url,
                urls=url_chunk,
                start_time=datetime.now(),
                last_ping=None,
                completion_time=None,
                intermediate_result_location=None,
                doc_id_start=doc_id_start,
            )
            self.task_monitor.register_task(task)

            try:
                response = requests.post(
                    f"{mapper_url}/map",
                    json={
                        "task_id": task_id,
                        "function_name": "process_wikipedia_urls",
                        "urls": url_chunk,
                        "doc_id_start": doc_id_start,
                    },
                    timeout=MAPPER_TIMEOUT,
                )

                if response.status_code == 202:
                    self.logger.info(
                        f"Task {task_id} successfully assigned and in progress for mapper {mapper_url} with {len(url_chunk)} URLs"
                    )
                    task.state = TASK_STATES["IN_PROGRESS"]

                    results.append(
                        {
                            "mapper_url": mapper_url,
                            "task_id": task_id,
                            "status": STATUS["SUCCESS"],
                            "urls_assigned": len(url_chunk),
                        }
                    )
                    doc_id_start += len(url_chunk)
                else:
                    task.state = TASK_STATES["FAILED"]
                    self.logger.error(
                        f"Failed to assign task to mapper {i}: {response.status_code}"
                    )
                    results.append(
                        {
                            "mapper_url": mapper_url,
                            "status": STATUS["ERROR"],
                            "task_id": task_id,
                            "error": f"HTTP {response.status_code}",
                        }
                    )

            except Exception as e:
                self.logger.error(f"Error communicating with mapper {i}: {e}")
                results.append(
                    {
                        "mapper_url": mapper_url,
                        "task_id": task_id,
                        "status": STATUS["ERROR"],
                        "error": str(e),
                    }
                )

        return results
