import json
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import requests

# ==============================================================================
# 1. PHONE & KAFKA CONFIGURATION
# ==============================================================================
# REPLACE this with the IP shown in your Phyphox app!
PHYPHOX_URL = "http://10.22.75.173:8080"  # Example: "http://10.22.75.173:8080"

KAFKA_BROKER = "172.17.204.151:9092"
KAFKA_TOPIC = "sensor_data"
DURATION_SECONDS = 180  # Stream for 3 minutes (change if desired)

# ==============================================================================
# 2. CONNECT TO KAFKA
# ==============================================================================
print(f"Connecting to Kafka broker at {KAFKA_BROKER}...")
try:
  producer = KafkaProducer(
      bootstrap_servers=[KAFKA_BROKER],
      value_serializer=lambda v: json.dumps(v).encode("utf-8"),
      request_timeout_ms=10000,
  )
  print("Connected to Kafka successfully!")
except NoBrokersAvailable:
  print(f"ERROR: Cannot reach Kafka on {KAFKA_BROKER}. Is it running in WSL?")
  raise SystemExit(1)

# ==============================================================================
# 3. STREAM FROM PHONE TO KAFKA
# ==============================================================================
print(f"Polling Phyphox at {PHYPHOX_URL} (Duration: {DURATION_SECONDS}s)...")
print("Move or shake your phone to send readings!")

last_time = None
sent_count = 0
start_time = time.time()

try:
  while (time.time() - start_time) < DURATION_SECONDS:
    try:
      # Poll the accelerometer buffer from Phyphox
      response = requests.get(
          f"{PHYPHOX_URL}/get?accX=full&accY=full&accZ=full&acc_time=full",
          timeout=3,
      )
      buf = response.json().get("buffer", {})

      times = buf.get("acc_time", {}).get("buffer", [])
      x_vals = buf.get("accX", {}).get("buffer", [])
      y_vals = buf.get("accY", {}).get("buffer", [])
      z_vals = buf.get("accZ", {}).get("buffer", [])

      new_points = 0
      for t, x, y, z in zip(times, x_vals, y_vals, z_vals):
        if None in (t, x, y, z):
          continue
        if last_time is not None and t <= last_time:
          continue

        payload = {"time": t, "acc_x": x, "acc_y": y, "acc_z": z}
        producer.send(KAFKA_TOPIC, value=payload)
        last_time = t
        sent_count += 1
        new_points += 1

      if new_points > 0:
        elapsed = time.time() - start_time
        print(
            f"Sent {new_points} new readings to Kafka (Total: {sent_count} |"
            f" Elapsed: {elapsed:.0f}s)"
        )

    except requests.exceptions.RequestException as e:
      print(f"Waiting for Phyphox... (Error: {e})")

    # Poll every 0.5s for smooth streaming
    time.sleep(0.5)

finally:
  producer.flush()
  print(f"\nProducer stopped. Total readings sent to Kafka: {sent_count}")
