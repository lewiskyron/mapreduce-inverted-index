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
import json

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

    def process_reduce_task(self, task_id: str, mapper_results: List[Dict], output_dir: str):
        """Process reduce task in a separate thread"""
        try:
            self.logger.info(f"Starting reduce task {task_id}")
            self.update_task_status(task_id, "in_progress")

            parsed_results = []
            for result in mapper_results:
                data_field = result.get('data')

                # If 'data' is present and is a string, attempt to parse as JSON
                if isinstance(data_field, str):
                    try:
                        parsed_data = json.loads(data_field)
                        parsed_results.append(parsed_data)
                    except json.JSONDecodeError as e:
                        # Log a warning and skip this specific result, rather than failing the whole task
                        self.logger.warning(
                            f"Failed to parse 'data' field as JSON. Skipping this result. Error: {e}"
                        )
                else:
                    # If 'data' is already a dictionary or if data_field doesn't exist, handle accordingly
                    if isinstance(data_field, dict):
                        parsed_results.append(data_field)
                    else:
                        # If there's no 'data' field or it's neither a string nor a dict,
                        # you can either skip or handle differently.
                        self.logger.warning(
                            "No parsable 'data' field found in mapper result. Skipping this result."
                        )

            inverted_index = self.processor.process_intermediate_results(parsed_results)
            logging.info(f"This is the length of the inverted_index{len(inverted_index)}")
            output_file = self.processor.save_final_index(inverted_index, output_dir)

            self.logger.info(
                f"Reduce task {task_id} completed successfully. Results saved to {output_file}"
            )
            self.update_task_status(task_id, "completed", output_file)
        except Exception as e:
            self.logger.error(f"Error processing reduce task {task_id}: {e}")
            self.update_task_status(task_id, "failed")

    def reduce_results(self):
        """Handle reduction requests"""
        try:
            data = request.get_json()
            task_id = data.get("task_id")
            intermediate_files = data.get("intermediate_files", [])
            output_dir = data.get("output_dir", OUTPUT_DIR_DEFAULT)

            if not task_id or not intermediate_files:
                self.logger.error(f"Missing parameters in reduce request. Got: {data}")
                return jsonify({
                    "error": "Missing required parameters", 
                    "received": data
                }), 400

            # First fetch the data from all mappers
            combined_results = []
            for file_info in intermediate_files:
                mapper_url = file_info.get("mapper_url")
                location = file_info.get("location")

                self.logger.info(f"Requesting data from mapper {mapper_url} at location {location}")
                try:
                    response = requests.get(
                        f"{mapper_url}/get_results",
                        params={"location": location},
                        timeout=30
                    )

                    if response.status_code == 200:
                        mapper_data = response.json()
                        self.logger.info(f"Successfully received data from mapper {mapper_url}")
                        combined_results.append(mapper_data)
                    else:
                        self.logger.error(f"Failed to get data from mapper {mapper_url}. Status: {response.status_code}")
                        return jsonify({
                            "error": f"Failed to fetch data from mapper {mapper_url}"
                        }), 500
                except Exception as e:
                    self.logger.error(f"Error requesting data from mapper {mapper_url}: {e}")
                    return jsonify({
                        "error": f"Error fetching data from mapper: {str(e)}"
                    }), 500

            # Now submit the task with the collected data
            self.executor.submit(
                self.process_reduce_task, 
                task_id, 
                combined_results,  # Pass the actual mapper data instead of just the file info
                output_dir
            )

            return jsonify({
                "status": "accepted",
                "task_id": task_id,
                "message": "Reduce task processing started",
                "files_count": len(intermediate_files),
                "mappers_data_received": len(combined_results)
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
