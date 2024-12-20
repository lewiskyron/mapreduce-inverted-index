FROM --platform=linux/amd64 python:3.9-slim

WORKDIR /app

# Copy the package files
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the source code
COPY src/ ./src/

# Create necessary directories
RUN mkdir -p /app/data/final

EXPOSE 5003

# Run using the module structure
CMD ["python", "-m", "src.reducer"]