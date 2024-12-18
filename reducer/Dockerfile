# reducer/Dockerfile
FROM --platform=linux/amd64 python:3.9-slim

WORKDIR /app

COPY reducer.py requirements.txt /app/

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 5003

CMD ["python", "reducer.py"]
