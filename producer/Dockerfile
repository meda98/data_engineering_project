# Lightweight Python base image
FROM python:3.11-slim

# Set working directory inside the container
WORKDIR /app

# Copy dependency definition
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy producer source code
COPY producer.py .

# Copy dataset into the image
COPY data ./data

# Start the Kafka producer
CMD ["python", "-u", "producer.py"]