import csv
import json
import os
import time
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# Kafka connection configuration
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9093")

# Kafka topic used for raw environmental sensor readings
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "environmental_sensor_readings")

# Input dataset and streaming parameters
CSV_PATH = os.environ.get("CSV_PATH", "./data/iot_telemetry_data.csv")
SLEEP_SECONDS = float(os.environ.get("SLEEP_SECONDS", "2"))

# Device identifier assigned to this producer instance
DEVICE_ID = os.environ.get("DEVICE_ID")

# Convert numeric values from CSV to float
def to_float(x):
    return float(x)

# Convert string representation of boolean values
def to_bool(x):
    if x == "true":
        return True
    if x == "false":
        return False

# Initialize Kafka producer with retry logic
producer = None
while producer is None:
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
    except NoBrokersAvailable:
        print("Kafka not ready. Retrying...")
        time.sleep(2)

print(f"Streaming from CSV: {CSV_PATH}")
print(f"Producer assigned to device: {DEVICE_ID}")

# Continuous streaming loop
while True:

    # Open dataset and create CSV reader
    with open(CSV_PATH, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)

        # Iterate over all rows in the dataset
        for row in reader:

            # Skip rows belonging to other devices
            if row.get("device") != DEVICE_ID:
                continue

            # Build event message from sensor readings
            event = {
                "device_id": row.get("device"),
                "event_ts": datetime.now(timezone.utc).isoformat(),
                "co": to_float(row.get("co")),
                "humidity": to_float(row.get("humidity")),
                "light": to_bool(row.get("light")),
                "lpg": to_float(row.get("lpg")),
                "motion": to_bool(row.get("motion")),
                "smoke": to_float(row.get("smoke")),
                "temp": to_float(row.get("temp")),
            }

            # Send event to Kafka topic
            producer.send(KAFKA_TOPIC, event)
            producer.flush()

            # Log transmitted event
            print(f"Sent device={event['device_id']} at {event['event_ts']}")

            # Delay to simulate periodic sensor measurements
            time.sleep(SLEEP_SECONDS)