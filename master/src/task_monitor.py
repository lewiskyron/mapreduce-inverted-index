# master/src/task_monitor.py
from dataclasses import dataclass
from datetime import datetime
import threading
import time
import requests
import logging
from typing import Dict, Optional, List
from .constants import WORKER_TIMEOUT, HEARTBEAT_INTERVAL, TASK_STATES, JOB_STATES
from .job_state import JobState


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


@dataclass
class WorkerState:
    """Represents the state of a worker (mapper or reducer)"""

    type: str  # "mapper" or "reducer"
    state: str  # "idle" or "working"
    last_ping: datetime
    current_task: Optional[str] = None


class TaskMonitor:
    def __init__(self, ping_interval: int = HEARTBEAT_INTERVAL):
        self.logger = logging.getLogger(__name__)
        self.tasks: Dict[str, TaskState] = {}
        self.jobs: Dict[str, JobState] = {}
        self.active_workers: Dict[str, WorkerState] = {}
        self.reducers: Dict[str, datetime] = {}  # reducer_url -> last_ping
        self.ping_interval = ping_interval
        self.monitoring = False
        self.monitor_thread = None

    def start_monitoring(self):
        """Start the monitoring thread"""
        self.monitoring = True
        self.monitor_thread = threading.Thread(target=self._monitoring_loop)
        self.logger.info("Starting monitoring thread")
        self.monitor_thread.daemon = True
        self.monitor_thread.start()

    def stop_monitoring(self):
        """Stop the monitoring thread"""
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join()
            self.logger.info("Monitoring thread stopped")

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
                    worker_state = self.active_workers[worker_url]
                    worker_state.last_ping = datetime.now()

                    response_data = response.json()
                    tasks = response_data.get("tasks", {})
                    if not tasks:
                        # No tasks means worker is idle
                        self.update_worker_state(worker_url, "idle", None)
                    else:
                        current_task_id = worker_state.current_task
                        if current_task_id and current_task_id in tasks:
                            task_state = tasks[current_task_id]["state"]
                            if task_state == "completed":
                                # Task finished, worker is idle again
                                self.update_worker_state(worker_url, "idle", None)
                            else:
                                # Task still in progress
                                self.update_worker_state(
                                    worker_url, "working", current_task_id
                                )
                    self._update_worker_tasks(worker_url, response.json())
                    self.logger.info(f"Received successful heartbeat from {worker_url}")
                else:
                    self._handle_worker_failure(worker_url)
            except Exception as e:
                self.logger.error(f"Error pinging worker {worker_url}: {e}")
                self._handle_worker_failure(worker_url)
    def register_worker(self, worker_url: str):
        """
        Register a new worker (mapper or reducer based on URL)
        Stores worker state in active_workers
        """
        current_time = datetime.now()
        is_reducer = "reducer" in worker_url

        # Create a proper WorkerState object
        worker_state = WorkerState(
            type="reducer" if is_reducer else "mapper",
            state="idle",
            last_ping=current_time,
            current_task=None
        )

        self.active_workers[worker_url] = worker_state
        self.logger.info(f"Registered new {worker_state.type} at {worker_url}")

        return {
            "status": "registered",
            "worker_url": worker_url,
            "type": worker_state.type
        }

    def get_available_reducers(self) -> List[str]:
        """Get list of idle reducer URLs"""
        return [
            url
            for url, state in self.active_workers.items()
            if state.type == "reducer" and state.state == "idle" 
        ]

    def update_worker_state(self, worker_url: str, state: str, task_id: str = None):
        """Update worker state (idle/working)"""
        if worker_url in self.active_workers:
            worker_state = self.active_workers[worker_url]
            worker_state.state = state
            worker_state.current_task = task_id
            self.logger.info(f"Updated {worker_url} state to {state}")

    def register_job(self, job: JobState):
        """Register a new MapReduce job"""
        self.jobs[job.job_id] = job
        self.logger.info(
            f"Registered new job {job.job_id} with {len(job.map_tasks)} map tasks"
        )

    def register_task(self, task: TaskState):
        """Register a new task"""
        self.tasks[task.task_id] = task
        self.logger.info(f"Registered new task {task.task_id}")

    def _check_and_initiate_reduce(self, job: JobState):
        """Check if map phase is complete and initiate reduce phase"""
        if job.all_map_tasks_completed(self.tasks):
            if not job.reducer_url:
                available_reducers = self.get_available_reducers()
                if not available_reducers:
                    self.logger.error(f"No reducers available for job {job.job_id}")
                    return
                job.reducer_url = available_reducers[0]
                self.logger.info(
                    f"Selected reducer {job.reducer_url} for job {job.job_id}"
                )

            # Collect information about intermediate results
            intermediate_results = []
            for task_id in job.map_tasks:
                task = self.tasks.get(task_id)
                if task and task.state == JOB_STATES["COMPLETED"]:
                    intermediate_results.append(
                        {
                            "task_id": task_id,
                            "location": task.intermediate_result_location,
                            "mapper_url": task.worker_url,
                        }
                    )

            try:
                response = requests.post(
                    f"{job.reducer_url}/reduce",
                    json={
                        "task_id": f"reduce_{job.job_id}",
                        "intermediate_files": intermediate_results,
                    },
                    timeout=30,
                )

                if response.status_code == 202:
                    self.logger.info(
                        f"Successfully initiated reduce phase for job {job.job_id}"
                    )
                    job.state = JOB_STATES["REDUCING"]
                else:
                    self.logger.error(
                        f"Failed to initiate reduce phase for job {job.job_id}"
                    )

            except Exception as e:
                self.logger.error(f"Error initiating reduce phase: {e}")

    def _handle_worker_failure(self, worker_url: str):
        self.logger.warning(f"Worker {worker_url} appears to be down")
        if worker_url in self.active_workers:
            worker_state = self.active_workers[worker_url]
            current_task_id = worker_state.current_task

            # If worker had a task, mark it as failed
            if current_task_id and current_task_id in self.tasks:
                task = self.tasks[current_task_id]
                if task.state == TASK_STATES["IN_PROGRESS"]:
                    task.state = TASK_STATES["FAILED"]
                    task.retries += 1

            # Remove the failed worker
            del self.active_workers[worker_url]

    def _update_worker_tasks(self, worker_url: str, status_data: Dict):
        """Update task states based on worker response"""
        for task_id, status in status_data.get("tasks", {}).items():
            if task_id in self.tasks:
                task = self.tasks[task_id]
                task.last_ping = datetime.now()
                
                # Get current state for comparison
                current_state = task.state
                new_state = status.get("state")
                
                # If task just completed
                if (
                    new_state == JOB_STATES["COMPLETED"] 
                    and current_state != JOB_STATES["COMPLETED"]
                ):
                    self.logger.info(f"Task {task_id} completed by worker {worker_url}")
                    task.state = JOB_STATES["COMPLETED"]
                    task.completion_time = datetime.now()
                    task.intermediate_result_location = status.get("result_location")
                    
                    # Check and update affected jobs
                    for job in self.jobs.values():
                        if task_id in job.map_tasks:
                            self.logger.info(f"Checking job {job.job_id} for reduce phase")
                            completed_tasks = len([
                                t for t in job.map_tasks 
                                if self.tasks[t].state == JOB_STATES["COMPLETED"]
                            ])
                            total_tasks = len(job.map_tasks)
                            self.logger.info(f"Job {job.job_id}: {completed_tasks}/{total_tasks} tasks completed")
                            self._check_and_initiate_reduce(job)

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

    def get_job_status(self) -> Dict:
        """Get status of all jobs"""
        return {
            job_id: {
                "state": job.state,
                "map_tasks_total": len(job.map_tasks),
                "map_tasks_completed": len(
                    [
                        task_id
                        for task_id in job.map_tasks
                        if self.tasks[task_id].state == JOB_STATES["COMPLETED"]
                    ]
                ),
                "start_time": job.start_time,
                "completion_time": job.completion_time,
                "reducer_url": job.reducer_url,
            }
            for job_id, job in self.jobs.items()
        }

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
