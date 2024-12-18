FROM python:3.9-slim

WORKDIR /app

# Install curl for healthcheck
RUN apt-get update && \
    apt-get install -y curl && \
    rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the source code
COPY . .

# Make sure the module is in Python path
ENV PYTHONPATH=/app

# Default command
CMD ["python", "-m", "src.master"]
