"""Central configuration for AeroPulse flight analytics."""

import os
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent

# Load environment variables from this assignment's local .env file.
load_dotenv(BASE_DIR / ".env")


def env_bool(name: str, default: bool = False) -> bool:
    """Parse a boolean environment variable safely."""
    value = os.getenv(name)

    if value is None:
        return default

    return value.strip().lower() in {"1", "true", "yes", "on"}


def env_int(name: str, default: int) -> int:
    """Parse an integer environment variable."""
    try:
        return int(os.getenv(name, str(default)))
    except ValueError as exc:
        raise ValueError(
            f"{name} must be an integer."
        ) from exc


# ---------------------------------------------------------------------
# DATA SOURCE: AVIATIONSTACK
# ---------------------------------------------------------------------

AVIATIONSTACK_KEY = os.getenv("AVIATIONSTACK_KEY", "").strip()

AVIATIONSTACK_URL = os.getenv(
    "AVIATIONSTACK_URL",
    "https://api.aviationstack.com/v1/flights",
)

USE_CACHE = env_bool("USE_CACHE", True)

_cache_path = Path(
    os.getenv("FLIGHT_CACHE_FILE", "flight_cache.json")
)

CACHE_FILE = str(
    _cache_path if _cache_path.is_absolute()
    else BASE_DIR / _cache_path
)

API_TIMEOUT_SECONDS = env_int("API_TIMEOUT_SECONDS", 20)


# ---------------------------------------------------------------------
# OPTIONAL DATA SOURCE: WEATHER
# ---------------------------------------------------------------------

OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY", "").strip()

OPENWEATHER_URL = (
    "https://api.openweathermap.org/data/2.5/weather"
)


# ---------------------------------------------------------------------
# AIRPORT REFERENCE: BENGALURU (BLR / VOBL)
# ---------------------------------------------------------------------

AIRPORT_IATA = "BLR"
AIRPORT_ICAO = "VOBL"

AIRPORT_LAT = 13.1986
AIRPORT_LON = 77.7066

AIRCRAFT_RADIUS_NM = 25


# ---------------------------------------------------------------------
# APACHE KAFKA
# ---------------------------------------------------------------------

KAFKA_BROKER = os.getenv(
    "KAFKA_BROKER",
    "172.17.204.151:9092",
)

KAFKA_TOPIC_FLIGHTS = "flight_stream"
KAFKA_TOPIC_ENRICHED = "flight_enriched"
KAFKA_TOPIC_SCHEDULES = "flight_schedules"
KAFKA_TOPIC_WEATHER = "airport_weather"
KAFKA_TOPIC_EVENTS = "airport_events"

KAFKA_CLIENT_ID = "aeropulse-flight-producer"

KAFKA_REQUEST_TIMEOUT_MS = 15000


# ---------------------------------------------------------------------
# POSTGRESQL
# ---------------------------------------------------------------------

POSTGRES_CONFIG = {
    "dbname": os.getenv("POSTGRES_DB", "phyphox_db"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", ""),
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": env_int("POSTGRES_PORT", 5432),
}


def validate_postgres_config() -> None:
    """Ensure required database credentials are configured."""
    if not POSTGRES_CONFIG["password"]:
        raise RuntimeError(
            "POSTGRES_PASSWORD is missing. "
            "Set it in the local Assignment 4 .env file."
        )


# ---------------------------------------------------------------------
# STREAMING AND BUSINESS RULES
# ---------------------------------------------------------------------

CRUISING_ALTITUDE_METERS = 8000.0

DELAY_ALERT_MINUTES = 30

FLIGHT_POLL_SECONDS = 60
WEATHER_POLL_SECONDS = 600

MAX_FLIGHT_BATCH_SIZE = 100