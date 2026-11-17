"""Flight Telemetry Kafka Producer with Smart API Quota Caching.

Fetches active flights from Aviationstack API or replays from local cache,
then streams each flight into the Kafka topic 'flight_stream'.
"""

import argparse
import json
import os
import sys
import time
from config import (
    AVIATIONSTACK_KEY,
    AVIATIONSTACK_URL,
    CACHE_FILE,
    KAFKA_BROKER,
    KAFKA_TOPIC_FLIGHTS,
    USE_CACHE,
)
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import requests


def fetch_flights_from_api():
  """Fetches live active flights from Aviationstack API (uses 1 API request)."""
  print("\n🌐 Fetching live flight data from Aviationstack API (1 credit)...")
  params = {
      "access_key": AVIATIONSTACK_KEY,
      "flight_status": "active",
      "limit": 100,  # Grab maximum allowed per call
  }
  try:
    response = requests.get(AVIATIONSTACK_URL, params=params, timeout=10)
    data = response.json()

    if "error" in data:
      print(f"❌ API Error: {data['error'].get('message', data['error'])}")
      sys.exit(1)

    # Save to local cache for free reuse
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
      json.dump(data, f, indent=2)
    print(f" Saved snapshot of {len(data.get('data', []))} flights to cache!")
    return data.get("data", [])

  except requests.RequestException as e:
    print(f"❌ Network error while reaching Aviationstack: {e}")
    sys.exit(1)


def load_flights_from_cache():
  """Loads previously cached flight data (0 API credits used)."""
  if not os.path.exists(CACHE_FILE):
    print(
        f"ℹ️ Cache file '{CACHE_FILE}' not found. Fetching initial snapshot..."
    )
    return fetch_flights_from_api()

  print(
      f"\n Loading flight data from local cache '{CACHE_FILE}' (0 credits"
      " used)..."
  )
  with open(CACHE_FILE, "r", encoding="utf-8") as f:
    data = json.load(f)
  return data.get("data", [])


def clean_flight_record(flight):
  """Normalizes nested Aviationstack JSON into a clean flat dictionary."""
  flight_info = flight.get("flight") or {}
  airline_info = flight.get("airline") or {}
  dep_info = flight.get("departure") or {}
  arr_info = flight.get("arrival") or {}
  live_info = flight.get("live") or {}

  return {
      "flight_iata": flight_info.get("iata") or "N/A",
      "flight_number": flight_info.get("number") or "N/A",
      "airline_name": airline_info.get("name") or "Unknown Airline",
      "flight_status": flight.get("flight_status") or "active",
      "dep_airport": dep_info.get("airport") or "Unknown Departure",
      "dep_iata": dep_info.get("iata") or "---",
      "arr_airport": arr_info.get("airport") or "Unknown Arrival",
      "arr_iata": arr_info.get("iata") or "---",
      "dep_delay": dep_info.get("delay") or 0,
      "arr_delay": arr_info.get("delay") or 0,
      "latitude": float(live_info.get("latitude") or 0.0),
      "longitude": float(live_info.get("longitude") or 0.0),
      "altitude": float(live_info.get("altitude") or 0.0),
      "speed_horizontal": float(live_info.get("speed_horizontal") or 0.0),
      "speed_vertical": float(live_info.get("speed_vertical") or 0.0),
      "is_ground": bool(live_info.get("is_ground", False)),
      "timestamp": (
          live_info.get("updated")
          or time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime())
      ),
  }


def stream_to_kafka(flights, delay_between_msgs=0.1):
  """Connects to Kafka and streams each flight record."""
  print(f"Connecting to Kafka broker at {KAFKA_BROKER}...")
  try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        request_timeout_ms=10000,
    )
    print(" Connected to Kafka successfully!")
  except NoBrokersAvailable:
    print(
        f"❌ Cannot connect to Kafka on {KAFKA_BROKER}. Is Kafka running in"
        " WSL?"
    )
    sys.exit(1)

  print(
      f"\n🚀 Streaming {len(flights)} flight records into topic"
      f" '{KAFKA_TOPIC_FLIGHTS}'...\n"
  )
  published = 0
  for f in flights:
    cleaned = clean_flight_record(f)
    if cleaned["flight_iata"] == "N/A":
      continue  # Skip unassigned flights

    producer.send(KAFKA_TOPIC_FLIGHTS, value=cleaned)
    published += 1

    print(
        f"  ✈️ [{cleaned['flight_iata']}] {cleaned['airline_name']} |"
        f" {cleaned['dep_iata']} ➔ {cleaned['arr_iata']} | Alt:"
        f" {cleaned['altitude']:.0f}m | Spd: {cleaned['speed_horizontal']:.0f}"
        f" km/h | DepDelay: {cleaned['dep_delay']}m"
    )
    time.sleep(delay_between_msgs)

  producer.flush()
  print(
      f"\n Completed! Successfully streamed {published} flight records into"
      f" Kafka topic '{KAFKA_TOPIC_FLIGHTS}'."
  )


if __name__ == "__main__":
  parser = argparse.ArgumentParser(
      description="Flight Telemetry Kafka Producer"
  )
  parser.add_argument(
      "--live",
      action="store_true",
      help="Force live fetch from Aviationstack API (uses 1 credit)",
  )
  parser.add_argument(
      "--delay",
      type=float,
      default=0.05,
      help="Delay in seconds between Kafka messages (default: 0.05s)",
  )
  args = parser.parse_args()

  # Choose live API or cached data
  if args.live or not USE_CACHE:
    flight_data = fetch_flights_from_api()
  else:
    flight_data = load_flights_from_cache()

  if not flight_data:
    print("⚠️ No flight records available to stream.")
    sys.exit(0)

  stream_to_kafka(flight_data, delay_between_msgs=args.delay)
