import json
import os
import threading
import time
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import psycopg2

import config

os.makedirs("data", exist_ok=True)
PUBLIC_STATE_FILE = os.path.join("data", "public_state.json")


def transform_public_weather(raw: dict) -> dict:
  temp_c = raw.get("temp_c", 0.0)
  feels_like_c = raw.get("feels_like_c", temp_c)
  wind_kmh = raw.get("wind_speed_kmh", 0.0)

  temp_f = round((temp_c * 9 / 5) + 32, 1)
  feels_like_f = round((feels_like_c * 9 / 5) + 32, 1)
  wind_mph = round(wind_kmh * 0.621371, 1)

  is_heat_warning = temp_c > config.HEAT_WARNING_THRESHOLD_CELSIUS

  if is_heat_warning:
    alert_level = "CRITICAL_HEAT_WARNING"
    alert_title = "⚠️ EXTREME HEAT ADVISORY (>32°C / >89.6°F)"
    alert_message = (
        f"Temperature in {raw['city']} has reached {temp_c}°C ({temp_f}°F)."
        " High risk of heat exhaustion. Stay indoors between 12 PM - 3 PM and"
        " drink plenty of water."
    )
  elif temp_c > 28.0:
    alert_level = "MODERATE"
    alert_title = "Warm Weather Notice"
    alert_message = (
        f"Warm day in {raw['city']} at {temp_c}°C ({temp_f}°F). Stay hydrated."
    )
  else:
    alert_level = "NORMAL"
    alert_title = "Comfortable Conditions"
    alert_message = (
        f"Weather conditions in {raw['city']} are within standard comfort"
        " ranges."
    )

  return {
      "city": raw.get("city"),
      "country": raw.get("country"),
      "timestamp": raw.get("timestamp"),
      "temp_c": round(temp_c, 1),
      "temp_f": temp_f,
      "feels_like_c": round(feels_like_c, 1),
      "feels_like_f": feels_like_f,
      "temp_min_c": round(raw.get("temp_min_c", temp_c), 1),
      "temp_max_c": round(raw.get("temp_max_c", temp_c), 1),
      "humidity": raw.get("humidity"),
      "pressure_hpa": raw.get("pressure_hpa"),
      "wind_speed_kmh": wind_kmh,
      "wind_speed_mph": wind_mph,
      "weather_condition": raw.get("weather_condition"),
      "weather_desc": raw.get("weather_desc", "").capitalize(),
      "weather_icon": raw.get("weather_icon"),
      "heat_warning": is_heat_warning,
      "alert_level": alert_level,
      "alert_title": alert_title,
      "alert_message": alert_message,
      "last_updated": time.strftime("%Y-%m-%d %H:%M:%S"),
  }


def run_public_consumer():
  try:
    consumer = KafkaConsumer(
        config.TOPIC_LIVE_WEATHER,
        bootstrap_servers=[config.KAFKA_BROKER],
        auto_offset_reset="earliest",
        group_id=config.GROUP_PUBLIC,
        api_version=(2, 5, 0),  # Prevents Windows socket probe crash
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
  except NoBrokersAvailable:
    print("[Consumer 1] Cannot connect to Kafka. Is it running in WSL?")
    return

  print(f"[Consumer 1] Listening to [{config.TOPIC_LIVE_WEATHER}]...")

  for msg in consumer:
    raw_data = msg.value
    city = raw_data.get("city", "Unknown")

    cities_state = {}
    if os.path.exists(PUBLIC_STATE_FILE):
      try:
        with open(PUBLIC_STATE_FILE, "r") as f:
          cities_state = json.load(f)
      except Exception:
        cities_state = {}

    transformed = transform_public_weather(raw_data)
    cities_state[city] = transformed

    # 1. Update local JSON state store
    with open(PUBLIC_STATE_FILE, "w") as f:
      json.dump(cities_state, f, indent=2)

    # 2. Save / Upsert to PostgreSQL for Apache Superset
    try:
      conn = psycopg2.connect(**config.POSTGRES_CONFIG)
      conn.autocommit = True
      cur = conn.cursor()
      upsert_sql = """
            INSERT INTO weather_public_live (
                city, country, temp_c, temp_f, feels_like_c, humidity, 
                pressure_hpa, wind_speed_kmh, heat_warning, alert_level, alert_message, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (city) DO UPDATE SET
                temp_c = EXCLUDED.temp_c,
                temp_f = EXCLUDED.temp_f,
                feels_like_c = EXCLUDED.feels_like_c,
                humidity = EXCLUDED.humidity,
                pressure_hpa = EXCLUDED.pressure_hpa,
                wind_speed_kmh = EXCLUDED.wind_speed_kmh,
                heat_warning = EXCLUDED.heat_warning,
                alert_level = EXCLUDED.alert_level,
                alert_message = EXCLUDED.alert_message,
                updated_at = CURRENT_TIMESTAMP;
            """
      cur.execute(
          upsert_sql,
          (
              transformed["city"],
              transformed["country"],
              transformed["temp_c"],
              transformed["temp_f"],
              transformed["feels_like_c"],
              transformed["humidity"],
              transformed["pressure_hpa"],
              transformed["wind_speed_kmh"],
              transformed["heat_warning"],
              transformed["alert_level"],
              transformed["alert_message"],
          ),
      )
      cur.close()
      conn.close()
    except Exception as e:
      print(f"[Consumer 1 DB Error] {e}")

    status_icon = (
        "🔥 [HEAT ALERT]" if transformed["heat_warning"] else "☀️ [OK]"
    )
    print(
        f"[Consumer 1] {status_icon} Processed {city}: {transformed['temp_c']}°C"
        f" | Warning: {transformed['heat_warning']}"
    )


def start_in_background():
  t = threading.Thread(target=run_public_consumer, daemon=True)
  t.start()
  return t


if __name__ == "__main__":
  run_public_consumer()