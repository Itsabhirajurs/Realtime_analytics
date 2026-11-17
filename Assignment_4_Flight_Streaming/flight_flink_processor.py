"""Apache Flink Real-Time Flight Telemetry & Analytics Processor.

Consumes flight stream from Kafka, computes flight phases, classifies delay
severities, detects anomalies, and syncs enriched records to PostgreSQL.
"""

from pathlib import Path
import sys
from config import KAFKA_BROKER, KAFKA_TOPIC_FLIGHTS, POSTGRES_CONFIG
import psycopg2
from pyflink.common import Configuration
from pyflink.table import EnvironmentSettings, TableEnvironment


def run_flight_processor():
  print("=" * 70)
  print("✈️  Starting PyFlink Flight Analytics Streaming Engine")
  print("=" * 70)

  # 1. Initialize Flink Streaming Table Environment with Web UI
  config = Configuration()
  config.set_string("rest.port", "8081")
  config.set_string("rest.bind-port", "8081-8085")

  env_settings = (
      EnvironmentSettings.new_instance()
      .with_configuration(config)
      .in_streaming_mode()
      .build()
  )
  t_env = TableEnvironment.create(env_settings)
  t_env.get_config().set("parallelism.default", "1")

  # 2. Link the Kafka SQL Connector JAR (from flink_experiment)
  jar_path = Path(
      "../flink_experiment/flink-sql-connector-kafka-5.0.0-2.2.jar"
  ).resolve()
  if not jar_path.exists():
    # Fallback to local check if run from different working dir
    jar_path = Path("flink-sql-connector-kafka-5.0.0-2.2.jar").resolve()

  print(f"📦 Loading Flink Kafka Connector: {jar_path.name}")
  t_env.get_config().set("pipeline.jars", jar_path.as_uri())

  # 3. Define the Kafka Ingestion Table
  t_env.execute_sql(f"""
    CREATE TABLE kafka_flights (
        flight_iata STRING,
        flight_number STRING,
        airline_name STRING,
        flight_status STRING,
        dep_airport STRING,
        dep_iata STRING,
        arr_airport STRING,
        arr_iata STRING,
        dep_delay INT,
        arr_delay INT,
        latitude DOUBLE,
        longitude DOUBLE,
        altitude DOUBLE,
        speed_horizontal DOUBLE,
        speed_vertical DOUBLE,
        is_ground BOOLEAN,
        `timestamp` STRING
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{KAFKA_TOPIC_FLIGHTS}',
        'properties.bootstrap.servers' = '{KAFKA_BROKER}',
        'properties.group.id' = 'flink_flight_analytics_group',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json'
    )
    """)
  print(f"📥 Subscribed to Kafka topic '{KAFKA_TOPIC_FLIGHTS}'.")

  # 4. Flink SQL Transformation: Compute Flight Phase & Delay Alerts
  # Uses CASE WHEN logic compiled into Flink JVM streaming bytecode
  transform_query = """
    SELECT 
        flight_iata,
        airline_name,
        flight_status,
        dep_airport,
        dep_iata,
        arr_airport,
        arr_iata,
        dep_delay,
        arr_delay,
        latitude,
        longitude,
        altitude,
        speed_horizontal,
        CASE 
            WHEN is_ground = TRUE OR altitude = 0.0 THEN 'ON_GROUND'
            WHEN altitude >= 8000.0 THEN 'CRUISING'
            WHEN speed_vertical > 1.5 THEN 'CLIMBING'
            WHEN speed_vertical < -1.5 THEN 'DESCENDING'
            ELSE 'EN_ROUTE'
        END AS flight_phase,
        CASE 
            WHEN dep_delay >= 30 OR arr_delay >= 30 THEN 'HIGH_DELAY'
            WHEN dep_delay >= 15 OR arr_delay >= 15 THEN 'MODERATE_DELAY'
            ELSE 'ON_TIME'
        END AS delay_status
    FROM kafka_flights
    """

  result_table = t_env.sql_query(transform_query)
  print("⚙️  Flink Air Traffic Control rulebook compiled.")

  # 5. Connect to PostgreSQL Sink
  print("🔌 Connecting to PostgreSQL...")
  conn = psycopg2.connect(**POSTGRES_CONFIG)
  conn.autocommit = True
  cur = conn.cursor()

  upsert_telemetry_sql = """
    INSERT INTO flight_telemetry (
        flight_iata, airline_name, flight_status, dep_airport, dep_iata, 
        arr_airport, arr_iata, dep_delay, arr_delay, latitude, longitude, 
        altitude_m, speed_kmh, flight_phase, delay_status, updated_at
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
    ON CONFLICT (flight_iata) DO UPDATE SET
        flight_status = EXCLUDED.flight_status,
        dep_delay = EXCLUDED.dep_delay,
        arr_delay = EXCLUDED.arr_delay,
        latitude = EXCLUDED.latitude,
        longitude = EXCLUDED.longitude,
        altitude_m = EXCLUDED.altitude_m,
        speed_kmh = EXCLUDED.speed_kmh,
        flight_phase = EXCLUDED.flight_phase,
        delay_status = EXCLUDED.delay_status,
        updated_at = CURRENT_TIMESTAMP;
    """

  insert_alert_sql = """
    INSERT INTO flight_alerts (flight_iata, airline_name, alert_type, alert_message)
    VALUES (%s, %s, %s, %s);
    """

  print("\n" + "=" * 70)
  print(
      "🚀 FLINK IS NOW PROCESSING LIVE FLIGHTS (Web UI: http://localhost:8081)"
  )
  print("=" * 70 + "\n")

  # 6. Stream and Sink loop
  with result_table.execute().collect() as results:
    for r in results:
      f_iata = r[0]
      airline = r[1]
      status = r[2]
      dep_apt = r[3]
      dep_iata = r[4]
      arr_apt = r[5]
      arr_iata = r[6]
      dep_del = r[7] or 0
      arr_del = r[8] or 0
      lat = r[9]
      lon = r[10]
      alt = r[11]
      spd = r[12]
      phase = r[13]
      delay_stat = r[14]

      # Update Telemetry Table in Postgres
      cur.execute(
          upsert_telemetry_sql,
          (
              f_iata,
              airline,
              status,
              dep_apt,
              dep_iata,
              arr_apt,
              arr_iata,
              dep_del,
              arr_del,
              lat,
              lon,
              alt,
              spd,
              phase,
              delay_stat,
          ),
      )

      # Log to console
      status_icon = (
          "🚨"
          if delay_stat == "HIGH_DELAY"
          else ("✈️" if phase == "CRUISING" else "🛫")
      )
      print(
          f"  {status_icon} [FLINK] Flight {f_iata} ({airline}) | {dep_iata} ➔"
          f" {arr_iata} | Phase: {phase:<10} | Delay: {delay_stat} (+{dep_del}m)"
      )

      # If high delay (>30m), trigger safety alert in flight_alerts table
      if delay_stat == "HIGH_DELAY":
        alert_msg = (
            f"Major departure delay of {dep_del} mins on {airline} flight"
            f" {f_iata} from {dep_iata} to {arr_iata}."
        )
        cur.execute(
            insert_alert_sql,
            (f_iata, airline, "HIGH_DEPARTURE_DELAY", alert_msg),
        )
        print(f"      🔔 [ALERT TRIGGERED] Saved to flight_alerts table!")


if __name__ == "__main__":
  try:
    run_flight_processor()
  except KeyboardInterrupt:
    print("\n🛑 Flight processor stopped gracefully.")
