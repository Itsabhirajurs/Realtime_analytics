import json
import os
import time
from flask import Flask, jsonify, render_template, request

import config
import consumer_farmer
import consumer_public
import weather_producer

app = Flask(__name__)

DATA_DIR = "data"
PUBLIC_STATE_FILE = os.path.join(DATA_DIR, "public_state.json")
FARMER_STATE_FILE = os.path.join(DATA_DIR, "farmer_state.json")


def load_state(filepath: str) -> dict:
  if os.path.exists(filepath):
    try:
      with open(filepath, "r") as f:
        return json.load(f)
    except Exception:
      return {}
  return {}


def find_city_match(state_dict: dict, city_query: str):
  """Fuzzy & case-insensitive matching for city names (e.g."""
  if not city_query or not state_dict:
    return None

  q = city_query.strip().lower()

  # 1. Exact match (case-insensitive)
  for name, val in state_dict.items():
    if name.lower() == q:
      return val

  # 2. Substring match
  for name, val in state_dict.items():
    if q in name.lower() or name.lower() in q:
      return val

  return None


@app.route("/")
def index():
  return render_template(
      "index.html",
      preset_cities=config.PRESET_CITIES,
      default_city=config.DEFAULT_CITY,
  )


@app.route("/api/weather", methods=["GET"])
def get_weather():
  city_query = request.args.get("city", config.DEFAULT_CITY).strip()

  public_all = load_state(PUBLIC_STATE_FILE)
  farmer_all = load_state(FARMER_STATE_FILE)

  city_public = find_city_match(public_all, city_query)
  city_farmer = find_city_match(farmer_all, city_query)

  # If not yet in state, fetch via producer & give consumers 0.5s to digest
  if not city_public or not city_farmer:
    try:
      weather_producer.fetch_and_produce_weather(city_query)
      time.sleep(0.5)  # allow background consumers to consume and write state
      public_all = load_state(PUBLIC_STATE_FILE)
      farmer_all = load_state(FARMER_STATE_FILE)
      city_public = find_city_match(public_all, city_query)
      city_farmer = find_city_match(farmer_all, city_query)
    except Exception as e:
      print(f"Error triggering on-demand fetch: {e}")

  # Fallback to Bengaluru or first city if API failed
  if not city_public and public_all:
    first_city = list(public_all.keys())[0]
    city_public = public_all[first_city]
    city_farmer = farmer_all.get(first_city, {})

  return jsonify({
      "public": city_public,
      "farmer": city_farmer,
      "cities_available": list(public_all.keys()),
  })


if __name__ == "__main__":
  # Launch Consumer 1 and Consumer 2 in background threads
  print("\n--- Starting Background Kafka Consumers ---")
  consumer_public.start_in_background()
  consumer_farmer.start_in_background()

  print("\n=======================================================")
  print("🌐 Weather Analytics Dashboard running on http://127.0.0.1:5000")
  print("=======================================================\n")
  app.run(host="127.0.0.1", port=5000, debug=False)
