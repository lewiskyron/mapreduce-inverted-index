from flask import Flask, jsonify, request
import logging
import sys
from typing import Dict, Any, List
import threading
import requests
import time
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from .processor import ReducerProcessor
from .constants import (
    MASTER_URL_DEFAULT,
    REDUCER_URL_DEFAULT,
    MAX_RETRIES,
    RETRY_DELAY,
    OUTPUT_DIR_DEFAULT,
    REDUCER_HOST,
    REDUCER_PORT,
)


class ReducerServer:
    def __init__(self):
        self.app = Flask(__name__)
        self.processor = ReducerProcessor()
        self.logger = logging.getLogger(__name__)
        self.reducer_id = int(os.getenv("REDUCER_ID", 0))
        self.master_url = os.getenv("MASTER_URL", MASTER_URL_DEFAULT)
        self.active_tasks = {}
        self.task_lock = threading.Lock()
        self.executor = ThreadPoolExecutor(max_workers=4)
        self.setup_routes()

    def setup_routes(self):
        """Initialize all routes for the reducer server"""
        self.app.route("/ping", methods=["GET"])(self.handle_ping)
        self.app.route("/reduce", methods=["POST"])(self.reduce_results)
        self.app.route("/request_data", methods=["POST"])(self.request_mapper_data)

    def handle_ping(self):
        """Handle ping request from master"""
        self.logger.info("Received heartbeat")
        with self.task_lock:
            return (
                jsonify(
                    {
                        "status": "alive",
                        "reducer_id": self.reducer_id,
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

    def request_mapper_data(self):
        """Request intermediate results from a specific mapper"""
        try:
            data = request.get_json()
            mapper_url = data.get("mapper_url")
            result_location = data.get("result_location")

            if not mapper_url or not result_location:
                return (
                    jsonify({"error": "mapper_url and result_location required"}),
                    400,
                )

            response = requests.get(
                f"{mapper_url}/get_results",
                params={"location": result_location},
                timeout=30,
            )

            if response.status_code == 200:
                return jsonify(response.json()), 200
            else:
                return jsonify({"error": "Failed to fetch mapper results"}), 500

        except Exception as e:
            self.logger.error(f"Error requesting mapper data: {e}")
            return jsonify({"error": str(e)}), 500

    def process_reduce_task(
        self, task_id: str, intermediate_files: List[Dict], output_dir: str
    ):
        """Process reduce task in a separate thread (simplified for testing)"""
        try:
            self.logger.info(f"Starting reduce task {task_id}")
            self.update_task_status(task_id, "in_progress")

            # Log the received files information
            self.logger.info(f"Received {len(intermediate_files)} files to process:")
            for file_info in intermediate_files:
                self.logger.info(f"  - Location: {file_info.get('location')}")
                self.logger.info(f"  - Mapper: {file_info.get('mapper_url')}")
                self.logger.info(f"  - Task ID: {file_info.get('task_id')}")

            # Simulate processing time
            time.sleep(2)

            # Simulate successful completion
            mock_output_file = f"{output_dir}/result_{task_id}.json"
            self.logger.info(f"Reduce task {task_id} processed successfully")
            self.update_task_status(task_id, "completed", mock_output_file)

        except Exception as e:
            self.logger.error(f"Error processing reduce task {task_id}: {e}")
            self.update_task_status(task_id, "failed")

    # def process_reduce_task(
    #     self, task_id: str, mapper_results: List[Dict], output_dir: str
    # ):
    #     """Process reduce task in a separate thread"""
    #     try:
    #         self.update_task_status(task_id, "in_progress")

    #         # Process intermediate results into final inverted index
    #         inverted_index = self.processor.process_intermediate_results(mapper_results)

    #         # Save final results
    #         output_file = self.processor.save_final_index(inverted_index, output_dir)
    #         self.update_task_status(task_id, "completed", output_file)

    #     except Exception as e:
    #         self.logger.error(f"Error processing reduce task {task_id}: {e}")
    #         self.update_task_status(task_id, "failed")

    def reduce_results(self):
        """Handle reduction requests"""
        try:
            data = request.get_json()
            task_id = data.get("task_id")
            # Change to match what master sends
            intermediate_files = data.get("intermediate_files", [])
            output_dir = data.get("output_dir", OUTPUT_DIR_DEFAULT)

            if not task_id or not intermediate_files:
                self.logger.error(f"Missing parameters in reduce request. Got: {data}")
                return jsonify({
                    "error": "Missing required parameters", 
                    "received": data
                }), 400

            # Submit task to thread pool
            self.executor.submit(
                self.process_reduce_task, 
                task_id, 
                intermediate_files,  # Pass the intermediate files
                output_dir
            )

            return jsonify({
                "status": "accepted",
                "task_id": task_id,
                "message": "Reduce task processing started",
                "files_count": len(intermediate_files)
            }), 202

        except Exception as e:
            self.logger.error(f"Error in reduce_results: {e}")
            return jsonify({"error": str(e)}), 500

    def update_task_status(self, task_id: str, state: str, result_location: str = None):
        """Update status of a task"""
        with self.task_lock:
            self.active_tasks[task_id] = {
                "state": state,
                "result_location": result_location,
                "last_updated": datetime.now().isoformat(),
            }

    def register_with_master(self):
        """Register this reducer with the master"""
        reducer_url = os.getenv("REDUCER_URL", REDUCER_URL_DEFAULT)

        for attempt in range(MAX_RETRIES):
            try:
                self.logger.info(
                    f"Attempting to register with master at {self.master_url} (attempt {attempt + 1}/{MAX_RETRIES})"
                )
                response = requests.post(
                    f"{self.master_url}/register_reducer",
                    json={"reducer_url": reducer_url},
                    timeout=5,
                )

                if response.status_code == 200:
                    self.logger.info(
                        f"Successfully registered with master as reducer {self.reducer_id}"
                    )
                    return True
                else:
                    self.logger.error(
                        f"Failed to register with master. Status: {response.status_code}"
                    )
            except Exception as e:
                self.logger.error(f"Error registering with master: {str(e)}")

            if attempt < MAX_RETRIES - 1:
                self.logger.info(f"Retrying in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)

        self.logger.error("Failed to register with master after all attempts")
        return False

    def run(self, host: str = "0.0.0.0", port: int = 5003):
        """Run the reducer server"""
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
    reducer_server = ReducerServer()
    reducer_server.run(host=REDUCER_HOST, port=REDUCER_PORT)
