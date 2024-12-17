# reducer/reducer.py
from flask import Flask, jsonify, request

app = Flask(__name__)

count = 0


@app.route("/count", methods=["POST"])
def count_messages():
    global count
    data = request.json
    if data and "message" in data:
        count += 1
        return jsonify({"current_count": count}), 200
    return jsonify({"error": "No message received"}), 400


@app.route("/test", methods=["GET"])
def test():
    return jsonify({"message": "Reducer is running!", "current_count": count}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5003)
