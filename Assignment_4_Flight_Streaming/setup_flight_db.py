"""Database setup script for Assignment 4: Flight Streaming Analytics.

Creates 'flight_telemetry' and 'flight_alerts' tables in PostgreSQL.
"""

from config import POSTGRES_CONFIG
import psycopg2

print("Connecting to PostgreSQL to initialize flight database tables...")

conn = psycopg2.connect(**POSTGRES_CONFIG)
conn.autocommit = True
cur = conn.cursor()

# 1. Main Telemetry Table: Stores real-time aircraft status and metrics
cur.execute("""
CREATE TABLE IF NOT EXISTS flight_telemetry (
    flight_iata VARCHAR(20) PRIMARY KEY,
    airline_name VARCHAR(100),
    flight_status VARCHAR(30),
    dep_airport VARCHAR(120),
    dep_iata VARCHAR(10),
    arr_airport VARCHAR(120),
    arr_iata VARCHAR(10),
    dep_delay INT,
    arr_delay INT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    altitude_m DOUBLE PRECISION,
    speed_kmh DOUBLE PRECISION,
    flight_phase VARCHAR(30),
    delay_status VARCHAR(30),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
""")
print(" Table 'flight_telemetry' is ready.")

# 2. Alerts Table: Stores critical events (High delays, anomalous descent, etc.)
cur.execute("""
CREATE TABLE IF NOT EXISTS flight_alerts (
    id SERIAL PRIMARY KEY,
    flight_iata VARCHAR(20),
    airline_name VARCHAR(100),
    alert_type VARCHAR(50),
    alert_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
""")
print(" Table 'flight_alerts' is ready.")

cur.close()
conn.close()
print("PostgreSQL Flight Schema successfully deployed!\n")
