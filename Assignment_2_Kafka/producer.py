import time
import json
import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

PHYPHOX_URL = "http://10.22.75.173:8080"   # update each session if IP changes
KAFKA_TOPIC = "sensor_data"
DURATION = 180  # 3 minutes

try:
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        request_timeout_ms=10000,
    )
except NoBrokersAvailable:
    print("ERROR: Cannot reach Kafka on localhost:9092.")
    raise SystemExit(1)

print(f"Connected to Kafka. Reading Phyphox at {PHYPHOX_URL}...")

last_time = None
sent_count = 0
start = time.time()

try:
    while (time.time() - start) < DURATION:
        try:
            r = requests.get(
                f"{PHYPHOX_URL}/get?accX=full&accY=full&accZ=full&acc_time=full",
                timeout=3,
            )
            buf = r.json().get("buffer", {})
            t = buf.get("acc_time", {}).get("buffer", [])
            x = buf.get("accX", {}).get("buffer", [])
            y = buf.get("accY", {}).get("buffer", [])
            z = buf.get("accZ", {}).get("buffer", [])

            new_points = 0
            for ti, xi, yi, zi in zip(t, x, y, z):
                if None in (ti, xi, yi, zi):
                    continue
                if last_time is not None and ti <= last_time:
                    continue
                payload = {"time": ti, "acc_x": xi, "acc_y": yi, "acc_z": zi}
                producer.send(KAFKA_TOPIC, value=payload)
                last_time = ti
                sent_count += 1
                new_points += 1

            print(f"Sent so far: {sent_count} (+{new_points} this poll, elapsed {time.time()-start:.0f}s)")

        except requests.exceptions.RequestException as e:
            print(f"Phyphox request failed: {e}")

        time.sleep(1.0)  # poll once per second, plenty for step detection
finally:
    producer.flush()
    print(f"\nProducer finished. Total messages sent: {sent_count}")