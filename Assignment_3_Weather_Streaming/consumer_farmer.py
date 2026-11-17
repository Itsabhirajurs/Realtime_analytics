from datetime import datetime
import json
import os
import threading
import time
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import psycopg2

import config

os.makedirs("data", exist_ok=True)
FARMER_STATE_FILE = os.path.join("data", "farmer_state.json")


def transform_farmer_forecast(raw_payload: dict) -> dict:
  city = raw_payload.get("city", "Unknown")
  country = raw_payload.get("country", "")
  forecast_list = raw_payload.get("forecast_list", [])

  # 1. Group 3-hour forecast chunks by day
  daily_buckets = {}
  for item in forecast_list:
    dt_txt = item.get("dt_txt", "")
    if not dt_txt:
      dt = datetime.fromtimestamp(item.get("dt", time.time()))
      date_key = dt.strftime("%Y-%m-%d")
    else:
      date_key = dt_txt.split(" ")[0]

    if date_key not in daily_buckets:
      daily_buckets[date_key] = []
    daily_buckets[date_key].append(item)

  # 2. Daily Agronomic Aggregations
  daily_forecast_matrix = []
  max_humidity_seen = 0.0

  for date_str, readings in daily_buckets.items():
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    day_name = date_obj.strftime("%A")
    formatted_date = date_obj.strftime("%b %d")

    temps = [r.get("main", {}).get("temp", 0.0) for r in readings]
    min_temp_c = round(min(temps), 1)
    max_temp_c = round(max(temps), 1)

    humidities = [r.get("main", {}).get("humidity", 0) for r in readings]
    avg_humidity = round(sum(humidities) / len(humidities), 1)
    max_humidity_seen = max(max_humidity_seen, avg_humidity)

    pop_vals = [r.get("pop", 0.0) for r in readings]
    rain_prob_pct = int(round(max(pop_vals) * 100))

    wind_speeds = [r.get("wind", {}).get("speed", 0.0) for r in readings]
    avg_wind_kmh = round((sum(wind_speeds) / len(wind_speeds)) * 3.6, 1)

    rain_amounts = [r.get("rain", {}).get("3h", 0.0) for r in readings]
    total_rain_mm = round(sum(rain_amounts), 1)

    spray_favorable = rain_prob_pct < 50 and avg_wind_kmh < 15.0
    spray_window = (
        "FAVORABLE"
        if spray_favorable
        else (
            "UNFAVORABLE (High Rain)"
            if rain_prob_pct >= 50
            else "UNFAVORABLE (High Wind)"
        )
    )

    daily_forecast_matrix.append({
        "date": date_str,
        "formatted_date": formatted_date,
        "day_name": day_name,
        "min_temp_c": min_temp_c,
        "max_temp_c": max_temp_c,
        "avg_humidity": avg_humidity,
        "rain_prob_pct": rain_prob_pct,
        "total_rain_mm": total_rain_mm,
        "avg_wind_kmh": avg_wind_kmh,
        "spray_window": spray_window,
        "spray_favorable": spray_favorable,
    })

  is_high_humidity_alert = (
      max_humidity_seen > config.FARMER_HIGH_HUMIDITY_THRESHOLD
  )
  current_day_humidity = (
      daily_forecast_matrix[0]["avg_humidity"]
      if daily_forecast_matrix
      else max_humidity_seen
  )

  if is_high_humidity_alert:
    humidity_alert_level = "CRITICAL_PATHOGEN_WARNING"
    humidity_alert_title = (
        f"🚨 HIGH HUMIDITY WARNING"
        f" (>{config.FARMER_HIGH_HUMIDITY_THRESHOLD}%) - DISEASE ALERT"
    )
    humidity_alert_msg = (
        f"Average humidity in {city} is {current_day_humidity}%. Highly"
        " favorable for fungal pathogens (downy mildew, leaf blight, rust)."
        " Delay overhead irrigation, increase field aeration, and inspect crop"
        " foliage."
    )
  elif current_day_humidity > 65:
    humidity_alert_level = "MODERATE"
    humidity_alert_title = "Moderate Humidity (65-80%)"
    humidity_alert_msg = (
        f"Humidity is {current_day_humidity}%. Standard crop moisture."
        " Routine field scouting advised."
    )
  else:
    humidity_alert_level = "OPTIMAL"
    humidity_alert_title = "Optimal Dry Humidity (<65%)"
    humidity_alert_msg = (
        f"Humidity is {current_day_humidity}%. Low risk of fungal infection."
        " Ensure adequate irrigation."
    )

  return {
      "city": city,
      "country": country,
      "current_humidity": current_day_humidity,
      "high_humidity_alert": is_high_humidity_alert,
      "humidity_alert_level": humidity_alert_level,
      "humidity_alert_title": humidity_alert_title,
      "humidity_alert_msg": humidity_alert_msg,
      "forecast_days": daily_forecast_matrix,
      "last_updated": time.strftime("%Y-%m-%d %H:%M:%S"),
  }


def run_farmer_consumer():
  """Continuous consumer loop with Windows socket protection & self-healing."""
  while True:
    try:
      print("[Consumer 2] Connecting to Kafka...")
      consumer = KafkaConsumer(
          config.TOPIC_FORECAST_WEATHER,
          bootstrap_servers=[config.KAFKA_BROKER],
          auto_offset_reset="earliest",
          group_id=config.GROUP_FARMER,
          api_version=(2, 5, 0),
          value_deserializer=lambda m: json.loads(m.decode("utf-8")),
          consumer_timeout_ms=1000,
      )
      print(f"[Consumer 2] Listening to [{config.TOPIC_FORECAST_WEATHER}]...")

      while True:
        records = consumer.poll(timeout_ms=1000)
        for tp, messages in records.items():
          for msg in messages:
            raw_payload = msg.value
            city = raw_payload.get("city", "Unknown")

            farmer_state = {}
            if os.path.exists(FARMER_STATE_FILE):
              try:
                with open(FARMER_STATE_FILE, "r") as f:
                  farmer_state = json.load(f)
              except Exception:
                farmer_state = {}

            transformed = transform_farmer_forecast(raw_payload)
            farmer_state[city] = transformed

            # 1. Update local JSON state
            with open(FARMER_STATE_FILE, "w") as f:
              json.dump(farmer_state, f, indent=2)

            # 2. Save Forecast Days to PostgreSQL weather_db
            try:
              conn = psycopg2.connect(**config.POSTGRES_CONFIG)
              conn.autocommit = True
              cur = conn.cursor()
              insert_sql = """
                            INSERT INTO weather_farmer_forecast (
                                city, forecast_date, day_name, min_temp_c, max_temp_c,
                                avg_humidity, rain_probability, spray_window, humidity_alert, updated_at
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP);
                            """
              for day in transformed["forecast_days"]:
                cur.execute(
                    insert_sql,
                    (
                        city,
                        day["formatted_date"],
                        day["day_name"],
                        day["min_temp_c"],
                        day["max_temp_c"],
                        day["avg_humidity"],
                        day["rain_prob_pct"],
                        day["spray_window"],
                        transformed["high_humidity_alert"],
                    ),
                )
              cur.close()
              conn.close()
            except Exception as e:
              print(f"[Consumer 2 DB Error] {e}")

            status_icon = (
                "🚨 [HUMIDITY ALERT]"
                if transformed["high_humidity_alert"]
                else "🌱 [AGRI OK]"
            )
            print(
                f"[Consumer 2] {status_icon} Processed {city}:"
                f" {len(transformed['forecast_days'])}-Day Forecast |"
                f" Humidity: {transformed['current_humidity']}% | Alert:"
                f" {transformed['high_humidity_alert']}"
            )
        time.sleep(0.1)

    except Exception as e:
      print(f"[Consumer 2] Reconnecting in 3s due to: {e}")
      time.sleep(3)


def start_in_background():
  t = threading.Thread(target=run_farmer_consumer, daemon=True)
  t.start()
  return t


if __name__ == "__main__":
  run_farmer_consumer()