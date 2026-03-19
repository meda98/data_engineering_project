import json
import os
import time
import psycopg2
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

# PostgreSQL connection configuration
PGHOST = os.environ["PGHOST"]
PGPORT = os.environ["PGPORT"]
PGDATABASE = os.environ["PGDATABASE"]
PGUSER = os.environ["PGUSER"]
PGPASSWORD = os.environ["PGPASSWORD"]

# Kafka connection configuration
KAFKA_BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"]
KAFKA_TOPIC = os.environ["KAFKA_TOPIC"]

# Connect to PostgreSQL with retry logic
def connect_db():
    while True:
        try:
            conn = psycopg2.connect(
                host=PGHOST,
                port=PGPORT,
                dbname=PGDATABASE,
                user=PGUSER,
                password=PGPASSWORD,
            )
            conn.autocommit = True
            return conn
        except Exception as e:
            print("Database not ready:", e, flush=True)
            time.sleep(2)

# Connect to PostgreSQL
conn = connect_db()
cur = conn.cursor()

# Create table for environmental alerts
cur.execute("""
CREATE TABLE IF NOT EXISTS environmental_alerts (
  id SERIAL PRIMARY KEY,
  alert_ts TIMESTAMPTZ,
  device_id TEXT,
  alert_type TEXT,
  previous_status TEXT,
  current_status TEXT,
  environmental_index_avg DOUBLE PRECISION
);
""")

# Initialize Kafka consumer with retry logic
consumer = None
while consumer is None:
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            group_id="alerts-consumer",
        )
    except NoBrokersAvailable:
        print("Kafka not ready. Retrying...", flush=True)
        time.sleep(2)

print(f"Alert consumer started. Reading from topic: {KAFKA_TOPIC}", flush=True)

# Read alert events from Kafka and store them in PostgreSQL
for msg in consumer:
    e = msg.value

    cur.execute(
        """
        INSERT INTO environmental_alerts (
            alert_ts,
            device_id,
            alert_type,
            previous_status,
            current_status,
            environmental_index_avg
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (
            e["alert_ts"],
            e["device_id"],
            e["alert_type"],
            e["previous_status"],
            e["current_status"],
            e["environmental_index_avg"],
        ),
    )

    print("Inserted alert:", e, flush=True)