import json
import os
import time
import psycopg2
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

# Connection parameters are provided via environment variables
PGHOST = os.environ["PGHOST"]
PGPORT = os.environ["PGPORT"]
PGDATABASE = os.environ["PGDATABASE"]
PGUSER = os.environ["PGUSER"]
PGPASSWORD = os.environ["PGPASSWORD"]

KAFKA_BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"]
KAFKA_TOPIC = os.environ["KAFKA_TOPIC"]

def connect_db():
    # Retry until PostgreSQL is reachable
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

# Create a table for sensor readings
cur.execute("""
CREATE TABLE IF NOT EXISTS sensor_readings (
  id SERIAL PRIMARY KEY,
  udi INT,
  air_temperature_k DOUBLE PRECISION,
  process_temperature_k DOUBLE PRECISION,
  rotational_speed_rpm INT,
  torque_nm DOUBLE PRECISION,
  tool_wear_min INT
);
""")

# Create a Kafka consumer instance
# Retry until Kafka is reachable
consumer = None
while consumer is None:
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            group_id="sensor-consumer",
        )
    except NoBrokersAvailable:
        print("Kafka not ready. Retrying...")
        time.sleep(2)

print("Consumer started.")

# Read messages from Kafka and insert them into the sensor readings table
for msg in consumer:
    e = msg.value
    cur.execute(
        """
        INSERT INTO sensor_readings (
            udi,
            air_temperature_k,
            process_temperature_k,
            rotational_speed_rpm,
            torque_nm,
            tool_wear_min
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (
            e.get("udi"),
            e.get("air_temperature_k"),
            e.get("process_temperature_k"),
            e.get("rotational_speed_rpm"),
            e.get("torque_nm"),
            e.get("tool_wear_min")
        )
    )
    print("Inserted:", e)