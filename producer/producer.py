import csv
import json
import os
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# Kafka connection settings (read from environment or use defaults)
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9093")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "sensor_readings")

# Input CSV file path and streaming delay
CSV_PATH = os.environ.get("CSV_PATH", "./data/predictive_maintenance.csv")
SLEEP_SECONDS = float(os.environ.get("SLEEP_SECONDS", "2"))

# Helper function to safely convert values to int
def to_int(x):
    return int(x) if x is not None and x != "" else None

# Helper function to safely convert values to float
def to_float(x):
    return float(x) if x is not None and x != "" else None

# Create a Kafka producer instance
# Retry until Kafka broker is available
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

# Continuously stream data from the CSV file
while True:
    # Open the CSV file on each loop to restart from the beginning
    with open(CSV_PATH, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)

        # Process each row as a separate event
        for row in reader:
            # Build the event payload with typed fields
            event = {
                "udi": to_int(row.get("UDI")),
                "air_temperature_k": to_float(row.get("Air temperature [K]")),
                "process_temperature_k": to_float(row.get("Process temperature [K]")),
                "rotational_speed_rpm": to_int(row.get("Rotational speed [rpm]")),
                "torque_nm": to_float(row.get("Torque [Nm]")),
                "tool_wear_min": to_int(row.get("Tool wear [min]"))
            }

            # Send event to Kafka topic
            producer.send(KAFKA_TOPIC, event)
            producer.flush()

            # Log progress
            print("Sent UDI:", event["udi"])

            # Pause between messages
            time.sleep(SLEEP_SECONDS)