import json
import math
import time
import psycopg2
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

KAFKA_TOPIC = "sensor_data"

print("Connecting to PostgreSQL...")
db = psycopg2.connect(dbname="phyphox_db", user="postgres",
                      password="admin123", host="localhost", port="5432")
cur = db.cursor()

print("Connecting to Kafka...")
try:
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        group_id=f"run_{int(time.time())}",     # fresh group every run -> always reads from start
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=30000,              # stop after 30s of silence instead of hanging forever
    )
except NoBrokersAvailable:
    print("ERROR: Cannot reach Kafka on localhost:9092. Is it running in WSL? (run 'jps')")
    raise SystemExit(1)

print("Listening... (will stop after 30s with no messages)")
count = 0
try:
    for msg in consumer:
        d = msg.value
        x, y, z = d['acc_x'], d['acc_y'], d['acc_z']
        mag = math.sqrt(x**2 + y**2 + z**2)
        cur.execute(
            "INSERT INTO accelerometer (record_time, acc_x, acc_y, acc_z, magnitude) "
            "VALUES (%s, %s, %s, %s, %s)",
            (d['time'], x, y, z, mag),
        )
        db.commit()
        count += 1
        if count % 50 == 0:
            print(f"Saved {count} rows (latest time {d['time']:.2f}s, mag {mag:.2f})")
except KeyboardInterrupt:
    print("\nStopped by user.")
finally:
    print(f"Total rows saved this run: {count}")
    cur.close()
    db.close()