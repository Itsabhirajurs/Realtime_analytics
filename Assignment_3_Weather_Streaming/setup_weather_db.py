"""Database setup script for Assignment 3: Weather Streaming Analytics.

Creates 'weather_public_live' and 'weather_farmer_forecast' tables in
PostgreSQL.
"""

from config import POSTGRES_CONFIG
import psycopg2

print("Connecting to PostgreSQL to initialize weather database tables...")

conn = psycopg2.connect(**POSTGRES_CONFIG)
conn.autocommit = True
cur = conn.cursor()

# 1. Public Weather Table: Stores latest weather metrics and heat warnings
cur.execute("""
CREATE TABLE IF NOT EXISTS weather_public_live (
    city VARCHAR(100) PRIMARY KEY,
    country VARCHAR(10),
    temp_c DOUBLE PRECISION,
    temp_f DOUBLE PRECISION,
    feels_like_c DOUBLE PRECISION,
    humidity INT,
    pressure_hpa INT,
    wind_speed_kmh DOUBLE PRECISION,
    heat_warning BOOLEAN,
    alert_level VARCHAR(50),
    alert_message TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
""")
print(" Table 'weather_public_live' is ready.")

# 2. Farmer Forecast Table: Stores multi-day agricultural forecast & spraying windows
cur.execute("""
CREATE TABLE IF NOT EXISTS weather_farmer_forecast (
    id SERIAL PRIMARY KEY,
    city VARCHAR(100),
    forecast_date VARCHAR(50),
    day_name VARCHAR(20),
    min_temp_c DOUBLE PRECISION,
    max_temp_c DOUBLE PRECISION,
    avg_humidity DOUBLE PRECISION,
    rain_probability INT,
    spray_window VARCHAR(50),
    humidity_alert BOOLEAN,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
""")
print(" Table 'weather_farmer_forecast' is ready.")

cur.close()
conn.close()
print("PostgreSQL Weather Schema successfully deployed!\n")