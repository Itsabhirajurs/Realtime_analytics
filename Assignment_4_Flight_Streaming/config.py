
"""AeroPulse - Assignment 4 configuration."""

import os
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

# Aviationstack
AVIATIONSTACK_KEY = os.getenv("AVIATIONSTACK_KEY", "")
AVIATIONSTACK_URL = os.getenv(
    "AVIATIONSTACK_URL",
    "http://api.aviationstack.com/v1/flights"
)

USE_CACHE = os.getenv("USE_CACHE", "true").lower() == "true"
CACHE_FILE = str(BASE_DIR / "flight_cache.json")

# Live ADS-B aircraft source
ADSBLOL_BASE_URL = "https://api.adsb.lol"

# Pilot airport: Bengaluru International Airport
AIRPORT_IATA = "BLR"
AIRPORT_ICAO = "VOBL"
AIRPORT_LAT = 13.1986
AIRPORT_LON = 77.7066
AIRCRAFT_RADIUS_NM = 25

# Weather
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY", "")
OPENWEATHER_URL = (
    "https://api.openweathermap.org/data/2.5/weather"
)

# Kafka
KAFKA_BROKER = os.getenv(
    "KAFKA_BROKER",
    "172.17.204.151:9092"
)

KAFKA_TOPIC_FLIGHTS = "flight_stream"
KAFKA_TOPIC_SCHEDULES = "flight_schedules"
KAFKA_TOPIC_WEATHER = "airport_weather"
KAFKA_TOPIC_EVENTS = "airport_events"

# PostgreSQL
POSTGRES_CONFIG = {
    "dbname": os.getenv("POSTGRES_DB", "phyphox_db"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", ""),
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
}

# Processing thresholds
CRUISING_ALTITUDE_METERS = 8000.0
DELAY_ALERT_MINUTES = 30

# Polling intervals
FLIGHT_POLL_SECONDS = 60
WEATHER_POLL_SECONDS = 600
