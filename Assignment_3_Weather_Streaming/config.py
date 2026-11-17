import os

OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")

if not OPENWEATHER_API_KEY:
    raise ValueError("OPENWEATHER_API_KEY is missing")

# Base API Endpoints
CURRENT_WEATHER_URL = "https://api.openweathermap.org/data/2.5/weather"
FORECAST_URL = "https://api.openweathermap.org/data/2.5/forecast"

# ==============================================================================
# KAFKA CLUSTER CONFIGURATION
# ==============================================================================
KAFKA_BROKER = "172.17.204.151:9092"
TOPIC_LIVE_WEATHER = "weather_live"
TOPIC_FORECAST_WEATHER = "weather_forecast"

# Consumer Groups (Two Distinct Groups for Independence)
GROUP_PUBLIC = "public_weather_group"
GROUP_FARMER = "farmer_weather_group"

# ==============================================================================
# THRESHOLDS & METRIC DEFINITIONS
# ==============================================================================
HEAT_WARNING_THRESHOLD_CELSIUS = 32.0  # Public warning if > 32°C
HIGH_HUMIDITY_THRESHOLD_PERCENT = 80.0  # Farmer alert if > 80% (fungal risk)
FARMER_HIGH_HUMIDITY_THRESHOLD = 80.0  # Alias for consumer_farmer

# Default City if none specified
DEFAULT_CITY = "Bengaluru"

# Popular Locations for Quick Switching
PRESET_CITIES = [
    "Bengaluru",
    "Mysuru",
    "Delhi",
    "Mumbai",
    "Chennai",
    "Hyderabad",
    "Kolkata",
    "Pune",
]

# ==============================================================================
# POSTGRESQL DATABASE CONFIGURATION (For Apache Superset)
# ==============================================================================
POSTGRES_CONFIG = {
    "dbname": "weather_db",  # Dedicated Weather Database
    "user": "postgres",
    "password": "admin123",
    "host": "localhost",
    "port": "5432",
}

