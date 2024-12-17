from flask import Flask, jsonify, request
import json
import sys
from mapper_logic import map_terms_to_documents
from mapper_logic import _nltk_diagnostic
import logging


app = Flask(__name__)

logging.basicConfig(
    level=logging.INFO,  # Change to DEBUG for more detailed logs
    format="%(asctime)s %(levelname)s:%(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)


@app.route("/echo", methods=["GET"])
def echo():
    return jsonify({"message": "Hello from Mapper!"}), 200


@app.route("/test", methods=["GET"])
def test():
    return jsonify({"message": "Mapper is running!"}), 200


@app.route("/map", methods=["POST"])
def map_terms():
    """
    Endpoint to receive a list of URLs and return mapped term-document pairs.
    Expects a JSON payload with a key 'urls' containing a list of URLs.
    """
    try:
        data = request.get_json()
        urls = data.get("urls", [])
        logging.info(len(urls))
        if not urls:
            return jsonify({"error": "No URLs provided."}), 400

        # Optional: Receive a starting document ID
        doc_id_start = data.get("doc_id_start", 0)
        logging.info(f"This is the doc_id: {doc_id_start}")

        # # Process the URLs
        mapped_terms = map_terms_to_documents(urls, doc_id_start)
        return jsonify({"mapped_terms": mapped_terms}), 200

        # return jsonify({"mapped_terms": mapped_terms}), 200
    except Exception as e:
        print(f"Error in /map endpoint: {e}", file=sys.stderr)
        return jsonify({"error": "An error occurred while processing."}), 500


if __name__ == "__main__":
    _nltk_diagnostic()
    app.run(host="0.0.0.0", port=5002)
