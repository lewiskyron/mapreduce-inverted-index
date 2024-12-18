# master/src/task_monitor.py
from dataclasses import dataclass
from datetime import datetime
import threading
import time
import requests
import logging
from typing import Dict, Optional, List
from .constants import WORKER_TIMEOUT, HEARTBEAT_INTERVAL, TASK_STATES


@dataclass
class TaskState:
    """Represents the state of a map task"""

    task_id: str
    state: str
    worker_url: Optional[str]
    urls: List[str]
    start_time: Optional[datetime]
    last_ping: Optional[datetime]
    completion_time: Optional[datetime]
    intermediate_result_location: Optional[str]
    retries: int = 0
    doc_id_start: int = 0


class TaskMonitor:

    def __init__(self, ping_interval: int = HEARTBEAT_INTERVAL):
        self.logger = logging.getLogger(__name__)
        self.tasks: Dict[str, TaskState] = {}
        self.active_workers: Dict[str, datetime] = (
            {}
        )  # worker_url -> last successful ping
        self.ping_interval = ping_interval
        self.monitoring = False
        self.monitor_thread = None

    def start_monitoring(self):
        """Start the monitoring thread"""
        self.monitoring = True
        self.monitor_thread = threading.Thread(target=self._monitoring_loop)
        logging.info("Attempting to send heartbeats")
        self.monitor_thread.daemon = True
        self.monitor_thread.start()

    def stop_monitoring(self):
        """Stop the monitoring thread"""
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join()

    def _monitoring_loop(self):
        """Main monitoring loop that pings workers"""
        while self.monitoring:
            self._ping_all_workers()
            time.sleep(self.ping_interval)

    def _ping_all_workers(self):
        """Ping all registered workers and update their status"""
        for worker_url in list(self.active_workers.keys()):
            try:
                self.logger.info(f"Sending heartbeat to worker at {worker_url}")
                response = requests.get(f"{worker_url}/ping", timeout=5)
                if response.status_code == 200:
                    self.active_workers[worker_url] = datetime.now()
                    self._update_worker_tasks(worker_url, response.json())
                    self.logger.info(
                        f"Received successful heartbeat response from {worker_url}"
                    )
                else:
                    self._handle_worker_failure(worker_url)
            except Exception as e:
                self.logger.error(f"Error pinging worker {worker_url}: {e}")
                self._handle_worker_failure(worker_url)

    def _update_worker_tasks(self, worker_url: str, status_data: Dict):
        """Update task states based on worker response"""
        for task_id, status in status_data.get("tasks", {}).items():
            if task_id in self.tasks:
                task = self.tasks[task_id]
                task.last_ping = datetime.now()
                if status.get("state") == TASK_STATES["COMPLETED"]:
                    task.completion_time = datetime.now()
                    task.intermediate_result_location = status.get("result_location")

    def _handle_worker_failure(self, worker_url: str):
        """Handle worker failure by marking its tasks as failed"""
        self.logger.warning(f"Worker {worker_url} appears to be down")
        affected_tasks = [
            task for task in self.tasks.values() if task.worker_url == worker_url
        ]

        for task in affected_tasks:
            if task.state == TASK_STATES["IN_PROGRESS"]:
                task.state = TASK_STATES["FAILED"]
                task.retries += 1

        if worker_url in self.active_workers:
            del self.active_workers[worker_url]

    def register_worker(self, worker_url: str):
        """Register a new worker"""
        self.active_workers[worker_url] = datetime.now()

    def register_task(self, task: TaskState):
        """Register a new task"""
        self.tasks[task.task_id] = task

    def get_worker_status(self) -> Dict:
        """Get status of all workers"""
        current_time = datetime.now()
        return {
            worker_url: {
                "last_ping": last_ping,
                "alive": (current_time - last_ping).seconds < WORKER_TIMEOUT,
                "tasks": len(
                    [t for t in self.tasks.values() if t.worker_url == worker_url]
                ),
            }
            for worker_url, last_ping in self.active_workers.items()
        }

    def get_task_status(self) -> Dict:
        """Get status of all tasks"""
        return {
            task_id: {
                "state": task.state,
                "worker": task.worker_url,
                "start_time": task.start_time,
                "last_ping": task.last_ping,
                "completion_time": task.completion_time,
                "retries": task.retries,
            }
            for task_id, task in self.tasks.items()
        }
