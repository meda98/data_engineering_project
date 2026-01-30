# Lightweight Python base image
FROM python:3.11-slim

# Set working directory inside the container
WORKDIR /app

# Copy dependency definition
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy consumer source code
COPY consumer.py .

# Start the Kafka consumer (unbuffered output for container logs)
CMD ["python", "-u", "consumer.py"]