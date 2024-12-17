# mapper/mapper.py
from flask import Flask, jsonify

app = Flask(__name__)


@app.route("/echo", methods=["GET"])
def echo():
    return jsonify({"message": "Hello from Mapper!"}), 200


@app.route("/test", methods=["GET"])
def test():
    return jsonify({"message": "Mapper is running!"}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5002)
