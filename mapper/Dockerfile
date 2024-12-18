FROM --platform=linux/amd64 python:3.9-slim

WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Download NLTK data in a single step
RUN python -m nltk.downloader stopwords

# Copy the source code
COPY . .

# Set Python path to recognize our module
ENV PYTHONPATH=/app

# Expose port 5002
EXPOSE 5002

# Use Gunicorn to run the Flask app with multiple workers
# Updated to use the module path
CMD ["gunicorn", "--workers=4", "--bind=0.0.0.0:5002", "src.mapper:app"]