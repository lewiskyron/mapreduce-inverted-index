from flask import Flask, jsonify
import requests
import logging
import os

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO)

# Get URLs from environment variables, with defaults for Docker Compose
MAPPER_URL = os.getenv("MAPPER_URL", "http://mapper:5002/echo")
REDUCER_URL = os.getenv("REDUCER_URL", "http://reducer:5003/count")


@app.route("/start", methods=["GET"])
def start_map():
    try:
        logging.info(f"Sending request to mapper at {MAPPER_URL}")
        response = requests.get(MAPPER_URL)
        data = response.json()
        logging.info(f"Received response from mapper: {data}")
        return jsonify({"master_received": data}), 200
    except requests.exceptions.RequestException as e:
        logging.error(f"Error communicating with mapper: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/test", methods=["GET"])
def test():
    return jsonify({"message": "Master is running!"}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)
