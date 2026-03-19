import json
import os
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

# Kafka connection configuration
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9093")

# Kafka topics
RAW_TOPIC = os.getenv("RAW_TOPIC", "environmental_sensor_readings")
PROCESSED_TOPIC = os.getenv("PROCESSED_TOPIC", "processed_environmental_readings")
ALERTS_TOPIC = os.getenv("ALERTS_TOPIC", "environmental_alerts")

# Consumer group identifier
GROUP_ID = os.getenv("GROUP_ID", "environmental-processor-group")

# Size of the moving average window
WINDOW_SIZE = int(os.getenv("WINDOW_SIZE", "3"))

# Weights used to compute the environmental index
CO_WEIGHT = float(os.getenv("CO_WEIGHT", "0.4"))
LPG_WEIGHT = float(os.getenv("LPG_WEIGHT", "0.3"))
SMOKE_WEIGHT = float(os.getenv("SMOKE_WEIGHT", "0.3"))

# Initialize Kafka consumer with retry logic
consumer = None
while consumer is None:
    try:
        consumer = KafkaConsumer(
            RAW_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            group_id=GROUP_ID,
        )
    except NoBrokersAvailable:
        print("[processor] Kafka not ready. Retrying...")
        time.sleep(2)

print(f"[processor] Consumer started. Reading from topic: {RAW_TOPIC}", flush=True)

# Initialize Kafka producer with retry logic
producer = None
while producer is None:
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
    except NoBrokersAvailable:
        print("[processor] Kafka not ready for producer. Retrying...")
        time.sleep(2)

print(f"[processor] Consuming {RAW_TOPIC}", flush=True)

# Rolling window storing recent environmental index values per device
env_index_window = defaultdict(lambda: deque(maxlen=WINDOW_SIZE))

# Store last known environmental index per device
last_status = {}

# Helper function to compute average of values
def avg(values):
    return sum(values) / len(values)

# Alert thresholds for environmental index with hysteresis
# To reduce repeated alerts caused by small fluctuations around threshold values
WARN_ON = float(os.getenv("ENV_INDEX_WARN_ON", "0.015"))
WARN_OFF = float(os.getenv("ENV_INDEX_WARN_OFF", "0.0145"))
CRIT_ON = float(os.getenv("ENV_INDEX_CRIT_ON", "0.020"))
CRIT_OFF = float(os.getenv("ENV_INDEX_CRIT_OFF", "0.019"))

# Determine status category based on threshold values
def get_status(environmental_index_avg, previous_status):
    if previous_status == "NORMAL":
        if environmental_index_avg >= CRIT_ON:
            return "CRITICAL"
        if environmental_index_avg >= WARN_ON:
            return "WARNING"
        return "NORMAL"

    if previous_status == "WARNING":
        if environmental_index_avg >= CRIT_ON:
            return "CRITICAL"
        if environmental_index_avg < WARN_OFF:
            return "NORMAL"
        return "WARNING"

    if previous_status == "CRITICAL":
        if environmental_index_avg < CRIT_OFF:
            if environmental_index_avg < WARN_OFF:
                return "NORMAL"
            return "WARNING"
        return "CRITICAL"

    return "NORMAL"

# Main stream processing loop
for msg in consumer:
    event = msg.value

    # Extract device identifier and sensor readings
    device_id = event["device_id"]
    co = event["co"]
    lpg = event["lpg"]
    smoke = event["smoke"]

    # Compute composite environmental index from gas-related sensors
    environmental_index = (
        CO_WEIGHT * co +
        LPG_WEIGHT * lpg +
        SMOKE_WEIGHT * smoke
    )

    # Update rolling window for the device
    env_index_window[device_id].append(environmental_index)

    # Compute moving average of the environmental index
    environmental_index_avg = avg(env_index_window[device_id])

    # Retrieve previously recorded status for the device
    previous_status = last_status.get(device_id, "NORMAL")

    # Determine current environmental index status
    current_status = get_status(environmental_index_avg, previous_status)

    # Enrich the original event with derived metrics
    processed_event = {
        **event,
        "processed_ts": datetime.now(timezone.utc).isoformat(),
        "environmental_index": round(environmental_index, 6),
        "environmental_index_avg": round(environmental_index_avg, 6),
        "status": current_status
    }

    # Publish enriched event to processed topic
    producer.send(PROCESSED_TOPIC, processed_event)
    producer.flush()

    # Emit alert only if the status changed
    if current_status != previous_status:

        alert = {
            "alert_ts": datetime.now(timezone.utc).isoformat(),
            "device_id": device_id,
            "alert_type": "ENVIRONMENTAL_INDEX_STATUS_CHANGE",
            "previous_status": previous_status,
            "current_status": current_status,
            "environmental_index_avg": round(environmental_index_avg, 6)
        }

        # Publish alert event
        producer.send(ALERTS_TOPIC, alert)
        producer.flush()

        # Update stored status for the device
        last_status[device_id] = current_status

        # Log alert event
        print(
            f"[processor] ALERT device={device_id} "
            f"{previous_status} -> {current_status} "
            f"env_index_avg={environmental_index_avg:.6f}",
            flush=True
        )