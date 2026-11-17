import json
import time
import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

import config

# ==============================================================================
# 1. INITIALIZE KAFKA PRODUCER
# ==============================================================================
print(f"Connecting to Kafka broker at {config.KAFKA_BROKER}...")
try:
  producer = KafkaProducer(
      bootstrap_servers=[config.KAFKA_BROKER],
      value_serializer=lambda v: json.dumps(v).encode("utf-8"),
      request_timeout_ms=10000,
  )
  print("Connected to Kafka successfully!")
except NoBrokersAvailable:
  print(f"ERROR: Cannot reach Kafka on {config.KAFKA_BROKER}.")
  raise SystemExit(1)


# ==============================================================================
# 2. WEATHER FETCHING FUNCTIONS
# ==============================================================================
def fetch_and_produce_weather(city: str = config.DEFAULT_CITY):
  """Fetches live weather & forecast from OpenWeatherMap API and publishes to Kafka topics."""
  print(f"\n[PRODUCER] Fetching weather data for: {city}...")

  # --- A. Fetch Live Current Weather ---
  live_params = {
      "q": city,
      "appid": config.OPENWEATHER_API_KEY,
      "units": "metric",  # Base measurements in Celsius & meters/sec
  }

  try:
    live_resp = requests.get(
        config.CURRENT_WEATHER_URL, params=live_params, timeout=5
    )
    if live_resp.status_code == 200:
      live_data = live_resp.json()

      # Package standard clean payload
      live_payload = {
          "city": live_data.get("name", city),
          "country": live_data.get("sys", {}).get("country", ""),
          "timestamp": live_data.get("dt", int(time.time())),
          "temp_c": live_data.get("main", {}).get("temp"),
          "feels_like_c": live_data.get("main", {}).get("feels_like"),
          "temp_min_c": live_data.get("main", {}).get("temp_min"),
          "temp_max_c": live_data.get("main", {}).get("temp_max"),
          "humidity": live_data.get("main", {}).get("humidity"),
          "pressure_hpa": live_data.get("main", {}).get("pressure"),
          "wind_speed_kmh": round(
              live_data.get("wind", {}).get("speed", 0) * 3.6, 1
          ),  # m/s to km/h
          "wind_deg": live_data.get("wind", {}).get("deg", 0),
          "weather_condition": (
              live_data["weather"][0]["main"]
              if live_data.get("weather")
              else "Clear"
          ),
          "weather_desc": (
              live_data["weather"][0]["description"]
              if live_data.get("weather")
              else ""
          ),
          "weather_icon": (
              live_data["weather"][0]["icon"]
              if live_data.get("weather")
              else "01d"
          ),
      }

      producer.send(config.TOPIC_LIVE_WEATHER, value=live_payload)
      print(
          f"  -> Published to [{config.TOPIC_LIVE_WEATHER}]:"
          f" {live_payload['city']} | Temp: {live_payload['temp_c']}°C |"
          f" Humidity: {live_payload['humidity']}%"
      )
    else:
      print(
          f"  [ERROR] Live weather request failed: {live_resp.status_code} -"
          f" {live_resp.text}"
      )

  except requests.RequestException as e:
    print(f"  [ERROR] Network error fetching live weather: {e}")

  # --- B. Fetch Multi-Day Forecast (For Farmer Dashboard) ---
  forecast_params = {
      "q": city,
      "appid": config.OPENWEATHER_API_KEY,
      "units": "metric",
  }

  try:
    fc_resp = requests.get(
        config.FORECAST_URL, params=forecast_params, timeout=5
    )
    if fc_resp.status_code == 200:
      fc_data = fc_resp.json()

      forecast_payload = {
          "city": fc_data.get("city", {}).get("name", city),
          "country": fc_data.get("city", {}).get("country", ""),
          "timestamp": int(time.time()),
          "forecast_list": fc_data.get("list", []),
      }

      producer.send(config.TOPIC_FORECAST_WEATHER, value=forecast_payload)
      print(
          f"  -> Published to [{config.TOPIC_FORECAST_WEATHER}]:"
          f" {forecast_payload['city']} ({len(forecast_payload['forecast_list'])} forecast periods)"
      )
    else:
      print(
          f"  [ERROR] Forecast request failed: {fc_resp.status_code} -"
          f" {fc_resp.text}"
      )

  except requests.RequestException as e:
    print(f"  [ERROR] Network error fetching forecast: {e}")

  producer.flush()


# ==============================================================================
# 3. RUN PRODUCER (POLLS PRESET CITIES & LOOPS)
# ==============================================================================
def run_producer_loop(poll_interval: int = 30):
  """Continuously polls weather for preset cities to simulate a live real-time stream."""
  print(
      f"\nStarting weather stream producer (Polling every {poll_interval}s)..."
  )
  try:
    while True:
      for city in config.PRESET_CITIES:
        fetch_and_produce_weather(city)
        time.sleep(2)  # small pause between cities to respect API rate limits

      print(
          f"\n--- Batch complete. Sleeping for {poll_interval}s before next"
          " refresh ---"
      )
      time.sleep(poll_interval)
  except KeyboardInterrupt:
    print("\nProducer stopped by user.")


if __name__ == "__main__":
  # Run once for testing or continuous loop
  import sys

  if len(sys.argv) > 1 and sys.argv[1] == "--once":
    city = sys.argv[2] if len(sys.argv) > 2 else config.DEFAULT_CITY
    fetch_and_produce_weather(city)
  else:
    run_producer_loop(poll_interval=45)
