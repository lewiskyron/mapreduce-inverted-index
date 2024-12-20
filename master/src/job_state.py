from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional
from .constants import JOB_STATES


@dataclass
class JobState:
    """Represents the state of an entire MapReduce job"""

    job_id: str
    map_tasks: List[str]  # List of map task IDs
    reduce_tasks: List[str]  # List of reduce task IDs
    state: str  # overall job state
    start_time: datetime
    completion_time: Optional[datetime] = None
    intermediate_locations: Dict[str, str] = None  # task_id -> location mapping
    reducer_url: Optional[str] = None

    def __post_init__(self):
        if self.intermediate_locations is None:
            self.intermediate_locations = {}

    def all_map_tasks_completed(self, task_states: Dict) -> bool:
        """Check if all map tasks are completed"""
        if not self.map_tasks:
            return False
            
        completed_count = 0
        total_count = len(self.map_tasks)
        
        for task_id in self.map_tasks:
            task = task_states.get(task_id)
            if task and task.state == JOB_STATES["COMPLETED"]:
                if task.intermediate_result_location:  # Ensure we have the result location
                    completed_count += 1
                
        return completed_count == total_count

    def get_completed_map_results(self, task_states: Dict) -> List[Dict]:
        """Get locations of all completed map task results"""
        results = []
        for task_id in self.map_tasks:
            if task_id in task_states:
                task = task_states[task_id]
                if (
                    task.state == JOB_STATES["COMPLETED"]
                    and task.intermediate_result_location
                ):
                    results.append(
                        {
                            "task_id": task_id,
                            "location": task.intermediate_result_location,
                            "worker_url": task.worker_url,
                        }
                    )
        return results
