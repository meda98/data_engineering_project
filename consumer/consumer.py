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
            print("Database not ready:", e)
            time.sleep(2)

# Connect to PostgreSQL
conn = connect_db()
cur = conn.cursor()

# Create table for processed environmental readings
cur.execute("""
CREATE TABLE IF NOT EXISTS processed_environmental_readings (
  id SERIAL PRIMARY KEY,
  device_id TEXT,
  event_ts TIMESTAMPTZ,
  processed_ts TIMESTAMPTZ,
  co DOUBLE PRECISION,
  humidity DOUBLE PRECISION,
  light BOOLEAN,
  lpg DOUBLE PRECISION,
  motion BOOLEAN,
  smoke DOUBLE PRECISION,
  temp DOUBLE PRECISION,
  environmental_index DOUBLE PRECISION,
  environmental_index_avg DOUBLE PRECISION,
  status TEXT
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
            group_id="processed-consumer",
        )
    except NoBrokersAvailable:
        print("Kafka not ready. Retrying...")
        time.sleep(2)

print(f"Consumer started. Reading from topic: {KAFKA_TOPIC}")

# Read processed events from Kafka and store them in PostgreSQL
for msg in consumer:
    e = msg.value

    cur.execute(
        """
        INSERT INTO processed_environmental_readings (
            device_id,
            event_ts,
            processed_ts,
            co,
            humidity,
            light,
            lpg,
            motion,
            smoke,
            temp,
            environmental_index,
            environmental_index_avg,
            status
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            e["device_id"],
            e["event_ts"],
            e["processed_ts"],
            e["co"],
            e["humidity"],
            e["light"],
            e["lpg"],
            e["motion"],
            e["smoke"],
            e["temp"],
            e["environmental_index"],
            e["environmental_index_avg"],
            e["status"],
        )
    )

    print("Inserted:", e)