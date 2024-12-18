from flask import Flask, jsonify, request
import logging
import sys
from typing import Dict, Any
from .available_functions import FunctionRegistry
from concurrent.futures import ThreadPoolExecutor
import os
from datetime import datetime
import threading
import requests
import time


class MapperServer:
    def __init__(self):
        self.app = Flask(__name__)
        self.function_registry = FunctionRegistry()
        self.logger = logging.getLogger(__name__)
        self.mapper_id = int(os.getenv("MAPPER_ID", 0))
        self.master_url = os.getenv("MASTER_URL", "http://master:5001")
        self.active_tasks = {}
        self.task_lock = threading.Lock()
        self.executor = ThreadPoolExecutor(max_workers=4)
        self.setup_routes()

    def setup_routes(self):
        """Initialize all routes for the mapper server"""
        self.app.route("/test", methods=["GET"])(self.test)
        self.app.route("/map", methods=["POST"])(self.map_terms)
        self.app.route("/ping", methods=["GET"])(self.handle_ping)

    def register_with_master(self):
        """Register this mapper with the master"""
        mapper_url = f"http://mapper:{5002}"  # Or get from environment
        max_retries = 5
        retry_delay = 3  # seconds

        for attempt in range(max_retries):
            try:
                self.logger.info(
                    f"Attempting to register with master at {self.master_url} (attempt {attempt + 1}/{max_retries})"
                )
                response = requests.post(
                    f"{self.master_url}/register_mapper",
                    json={"mapper_url": mapper_url},
                    timeout=5,
                )
                if response.status_code == 200:
                    self.logger.info("Successfully registered with master")
                    return True
                else:
                    self.logger.error(
                        f"Failed to register with master. Status: {response.status_code}"
                    )
            except Exception as e:
                self.logger.error(f"Error registering with master: {e}")

            if attempt < max_retries - 1:  # Don't sleep after the last attempt
                self.logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)

        self.logger.error("Failed to register with master after all attempts")
        return False

    def handle_ping(self):
        """Handle ping request from master"""
        self.logger.info(f"Received heartbeat from master at {self.master_url}")
        with self.task_lock: 
            return (
                jsonify(
                    {
                        "status": "alive",
                        "mapper_id": self.mapper_id,
                        "tasks": {
                            task_id: {
                                "state": status["state"],
                                "result_location": status.get("result_location"),
                                "progress": status.get("progress", 0),
                            }
                            for task_id, status in self.active_tasks.items()
                        },
                    }
                ),
                200,
            )

    def update_task_status(self, task_id: str, state: str, result_location: str = None):
        """Update status of a task"""
        with self.task_lock:
            self.active_tasks[task_id] = {
                "state": state,
                "result_location": result_location,
                "last_updated": datetime.now().isoformat(),
            }

    def test(self):
        return jsonify({"message": "Mapper is running!"}), 200

    def process_map_task(
        self, task_id: str, function_name: str, urls: list, doc_id_start: int
    ):
        """Process map task in a separate thread"""
        try:
            self.update_task_status(task_id, "in_progress")

            # Get the mapping function and execute it
            map_function = self.function_registry.get_function(function_name)
            mapped_terms = map_function(urls, doc_id_start)

            # Save results and update status
            intermediate_file_location = (
                self.function_registry.processor.save_intermediate_results(
                    mapped_terms, self.mapper_id
                )
            )
            self.update_task_status(task_id, "completed", intermediate_file_location)

        except Exception as e:
            self.logger.error(f"Error processing task {task_id}: {e}")
            self.update_task_status(task_id, "failed")

    def map_terms(self):
        """Handle mapping requests"""
        try:
            data = request.get_json()
            validation_result = self._validate_request(data)
            task_id = data.get("task_id")
            function_name = data.get("function_name")
            urls = data.get("urls", [])
            doc_id_start = data.get("doc_id_start", 0)

            if validation_result:
                return validation_result

            # Submit task to thread pool
            self.executor.submit(
                self.process_map_task, task_id, function_name, urls, doc_id_start
            )

            # function_name = data["function_name"]
            # urls = data["urls"]
            # doc_id_start = data.get("doc_id_start", 0)

            # map_function = self.function_registry.get_function(function_name)
            # self.logger.info(f"Executing {function_name} on {len(urls)} URLs")

            # mapped_terms = map_function(urls, doc_id_start)
            # intermediate_file = (
            #     self.function_registry.processor.save_intermediate_results(
            #         mapped_terms, self.mapper_id
            #     )
            # )
            return (
                jsonify(
                    {
                        "status": "accepted",
                        "task_id": task_id,
                        "message": "Task processing started",
                    }
                ),
                202,
            )

        except Exception as e:
            self.logger.error(f"Error in map_terms: {e}", exc_info=True)
            return jsonify({"error": str(e)}), 500

    def _validate_request(self, data: Dict[str, Any]):
        """Validate incoming request data"""
        if not data:
            return jsonify({"error": "No data provided"}), 400
        if "function_name" not in data:
            return jsonify({"error": "No function name provided"}), 400
        if "urls" not in data or not data["urls"]:
            return jsonify({"error": "No URLs provided"}), 400
        return None

    def run(self, host: str = "0.0.0.0", port: int = 5002):
        """Run the mapper server"""
        registration_thread = threading.Thread(target=self.register_with_master)
        registration_thread.daemon = True
        registration_thread.start()
        self.app.run(host=host, port=port, threaded=True)


def setup_logging():
    """Configure logging for the application"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s:%(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


if __name__ == "__main__":
    setup_logging()
    mapper_server = MapperServer()
    mapper_server.run()
