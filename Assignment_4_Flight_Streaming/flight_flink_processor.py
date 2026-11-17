"""AeroPulse: Continuous flight analytics using Apache Flink SQL."""

import logging
from pathlib import Path

from pyflink.common import Configuration
from pyflink.table import EnvironmentSettings, TableEnvironment

from config import (
    KAFKA_BROKER,
    KAFKA_TOPIC_FLIGHTS,
    KAFKA_TOPIC_ENRICHED,
)


# ---------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger("aeropulse.flink")


# ---------------------------------------------------------------------
# FLINK AND CONNECTOR CONFIGURATION
# ---------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parent.parent

CONNECTOR_JAR = (
    REPO_ROOT
    / "flink_experiment"
    / "flink-sql-connector-kafka-5.0.0-2.2.jar"
)

JOB_NAME = "AeroPulse Flight Enrichment"

CONSUMER_GROUP_ID = "aeropulse_flink_flight_processor"


# ---------------------------------------------------------------------
# KAFKA SOURCE TABLE
# ---------------------------------------------------------------------

SOURCE_DDL = f"""
CREATE TABLE flight_raw (

    event_id STRING,
    flight_identity STRING,
    flight_key STRING,

    `source` STRING,
    data_mode STRING,

    is_cache_replay BOOLEAN,
    is_fresh_api_snapshot BOOLEAN,

    source_snapshot_at STRING,
    source_observed_at STRING,
    ingested_at STRING,

    flight_iata STRING,
    flight_number STRING,
    flight_icao STRING,

    airline_name STRING,
    airline_iata STRING,
    airline_icao STRING,

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

    icao24 STRING,
    registration STRING,
    aircraft_type STRING,

    flight_date STRING,

    altitude_m DOUBLE,
    speed_kmh DOUBLE,
    vertical_rate_source_units DOUBLE,
    heading_degrees DOUBLE,

    has_position BOOLEAN,
    source_observation_time_known BOOLEAN,

    dep_terminal STRING,
    dep_gate STRING,
    dep_scheduled STRING,
    dep_estimated STRING,
    dep_actual STRING,
    dep_actual_runway STRING,

    arr_terminal STRING,
    arr_gate STRING,
    arr_baggage STRING,
    arr_scheduled STRING,
    arr_estimated STRING,
    arr_actual STRING,
    arr_actual_runway STRING,

    quality_flags ARRAY<STRING>

)
WITH (
    'connector' = 'kafka',
    'topic' = '{KAFKA_TOPIC_FLIGHTS}',
    'properties.bootstrap.servers' = '{KAFKA_BROKER}',
    'properties.group.id' = '{CONSUMER_GROUP_ID}',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'false'
)
"""


# ---------------------------------------------------------------------
# KAFKA ENRICHED SINK TABLE
# ---------------------------------------------------------------------

SINK_DDL = f"""
CREATE TABLE flight_enriched_sink (

    event_id STRING,
    flight_identity STRING,
    flight_key STRING,

    `source` STRING,
    data_mode STRING,

    is_cache_replay BOOLEAN,
    is_fresh_api_snapshot BOOLEAN,

    source_snapshot_at STRING,
    source_observed_at STRING,
    ingested_at STRING,

    flight_iata STRING,
    flight_number STRING,
    flight_icao STRING,

    airline_name STRING,
    airline_iata STRING,
    airline_icao STRING,

    flight_status STRING,

    dep_airport STRING,
    dep_iata STRING,

    arr_airport STRING,
    arr_iata STRING,

    dep_delay INT,
    arr_delay INT,

    delay_status STRING,
    delay_data_quality STRING,

    latitude DOUBLE,
    longitude DOUBLE,

    altitude_m DOUBLE,
    speed_kmh DOUBLE,
    vertical_rate_source_units DOUBLE,
    heading_degrees DOUBLE,

    is_ground BOOLEAN,

    icao24 STRING,
    registration STRING,
    aircraft_type STRING,

    flight_date STRING,

    has_position BOOLEAN,
    source_observation_time_known BOOLEAN,

    dep_terminal STRING,
    dep_gate STRING,
    dep_scheduled STRING,
    dep_estimated STRING,
    dep_actual STRING,
    dep_actual_runway STRING,

    arr_terminal STRING,
    arr_gate STRING,
    arr_baggage STRING,
    arr_scheduled STRING,
    arr_estimated STRING,
    arr_actual STRING,
    arr_actual_runway STRING,

    flight_phase STRING,
    telemetry_quality STRING,
    telemetry_anomaly STRING,

    is_blr_route BOOLEAN,

    quality_flags ARRAY<STRING>,

    processed_at TIMESTAMP_LTZ(3)

)
WITH (
    'connector' = 'kafka',
    'topic' = '{KAFKA_TOPIC_ENRICHED}',
    'properties.bootstrap.servers' = '{KAFKA_BROKER}',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601'
)
"""


# ---------------------------------------------------------------------
# FLINK SQL TRANSFORMATION
# ---------------------------------------------------------------------

INSERT_SQL = """

INSERT INTO flight_enriched_sink

SELECT

    event_id,
    flight_identity,
    flight_key,

    `source`,
    data_mode,

    is_cache_replay,
    is_fresh_api_snapshot,

    source_snapshot_at,
    source_observed_at,
    ingested_at,

    flight_iata,
    flight_number,
    flight_icao,

    airline_name,
    airline_iata,
    airline_icao,

    flight_status,

    dep_airport,
    dep_iata,

    arr_airport,
    arr_iata,

    dep_delay,
    arr_delay,

    -- ---------------------------------------------------------------
    -- 1. DELAY SEVERITY CLASSIFICATION
    -- ---------------------------------------------------------------

    CASE
        WHEN dep_delay >= 30 OR arr_delay >= 30
            THEN 'HIGH_DELAY'

        WHEN dep_delay >= 15 OR arr_delay >= 15
            THEN 'MODERATE_DELAY'

        WHEN dep_delay > 0 OR arr_delay > 0
            THEN 'LOW_DELAY'

        WHEN dep_delay IS NOT NULL AND arr_delay IS NOT NULL
            THEN 'ON_TIME'

        ELSE 'UNKNOWN'
    END AS delay_status,

    -- ---------------------------------------------------------------
    -- 2. DELAY DATA COMPLETENESS
    -- ---------------------------------------------------------------

    CASE
        WHEN dep_delay IS NOT NULL
             AND arr_delay IS NOT NULL
            THEN 'COMPLETE'

        WHEN dep_delay IS NOT NULL
             OR arr_delay IS NOT NULL
            THEN 'PARTIAL'

        ELSE 'MISSING'
    END AS delay_data_quality,

    -- ---------------------------------------------------------------
    -- 3. TELEMETRY
    -- ---------------------------------------------------------------

    latitude,
    longitude,

    altitude_m,
    speed_kmh,
    vertical_rate_source_units,
    heading_degrees,

    is_ground,

    icao24,
    registration,
    aircraft_type,

    flight_date,

    has_position,
    source_observation_time_known,

    -- ---------------------------------------------------------------
    -- 4. DEPARTURE OPERATIONS
    -- ---------------------------------------------------------------

    dep_terminal,
    dep_gate,
    dep_scheduled,
    dep_estimated,
    dep_actual,
    dep_actual_runway,

    -- ---------------------------------------------------------------
    -- 5. ARRIVAL OPERATIONS
    -- ---------------------------------------------------------------

    arr_terminal,
    arr_gate,
    arr_baggage,
    arr_scheduled,
    arr_estimated,
    arr_actual,
    arr_actual_runway,

    -- ---------------------------------------------------------------
    -- 6. FLIGHT PHASE CLASSIFICATION
    -- ---------------------------------------------------------------

    CASE
        WHEN is_ground = TRUE
            THEN 'ON_GROUND'

        WHEN altitude_m IS NULL
            THEN 'UNKNOWN'

        WHEN vertical_rate_source_units IS NULL
            THEN 'ALTITUDE_KNOWN_RATE_UNKNOWN'

        WHEN vertical_rate_source_units > 0
            THEN 'CLIMBING'

        WHEN vertical_rate_source_units < 0
            THEN 'DESCENDING'

        WHEN altitude_m >= 8000.0
             AND vertical_rate_source_units = 0
            THEN 'CRUISING'

        WHEN vertical_rate_source_units = 0
            THEN 'EN_ROUTE'

        ELSE 'UNKNOWN'
    END AS flight_phase,

    -- ---------------------------------------------------------------
    -- 7. TELEMETRY QUALITY
    -- ---------------------------------------------------------------

    CASE
        WHEN COALESCE(has_position, FALSE) = FALSE
            THEN 'POSITION_MISSING'

        WHEN altitude_m IS NULL OR speed_kmh IS NULL
            THEN 'PARTIAL_TELEMETRY'

        WHEN vertical_rate_source_units IS NULL
            THEN 'VERTICAL_RATE_MISSING'

        ELSE 'COMPLETE_TELEMETRY'
    END AS telemetry_quality,

    -- ---------------------------------------------------------------
    -- 8. BASIC TELEMETRY ANOMALY SCREENING
    -- ---------------------------------------------------------------

    CASE
        WHEN latitude IS NOT NULL
             AND (latitude < -90 OR latitude > 90)
            THEN 'INVALID_LATITUDE'

        WHEN longitude IS NOT NULL
             AND (longitude < -180 OR longitude > 180)
            THEN 'INVALID_LONGITUDE'

        WHEN altitude_m IS NOT NULL
             AND (altitude_m < -500 OR altitude_m > 20000)
            THEN 'ALTITUDE_OUT_OF_RANGE'

        WHEN speed_kmh IS NOT NULL
             AND (speed_kmh < 0 OR speed_kmh > 1500)
            THEN 'SPEED_OUT_OF_RANGE'

        WHEN is_ground = TRUE
             AND altitude_m > 1000
            THEN 'GROUND_ALTITUDE_CONFLICT'

        ELSE 'NONE_DETECTED'
    END AS telemetry_anomaly,

    -- ---------------------------------------------------------------
    -- 9. BENGALURU ROUTE IDENTIFICATION
    -- ---------------------------------------------------------------

    CASE
        WHEN dep_iata = 'BLR' OR arr_iata = 'BLR'
            THEN TRUE

        ELSE FALSE
    END AS is_blr_route,

    -- ---------------------------------------------------------------
    -- 10. DATA QUALITY FLAGS
    -- ---------------------------------------------------------------

    quality_flags,

    -- ---------------------------------------------------------------
    -- 11. FLINK PROCESSING TIMESTAMP
    -- ---------------------------------------------------------------

    CURRENT_TIMESTAMP AS processed_at

FROM flight_raw

"""


# ---------------------------------------------------------------------
# FLINK ENVIRONMENT
# ---------------------------------------------------------------------

def build_table_environment() -> TableEnvironment:
    """Create a streaming Flink TableEnvironment."""

    if not CONNECTOR_JAR.exists():
        raise FileNotFoundError(
            "Kafka SQL connector JAR not found.\n"
            f"Expected location: {CONNECTOR_JAR}\n"
            "Check the connector JAR path before starting Flink."
        )

    if not KAFKA_BROKER.strip():
        raise ValueError("KAFKA_BROKER is not configured.")

    config = Configuration()

    config.set_string("pipeline.name", JOB_NAME)
    config.set_string("parallelism.default", "1")

    config.set_string("rest.port", "8081")
    config.set_string("rest.bind-port", "8081-8085")

    # Link the connector JAR to this Flink job.
    config.set_string(
        "pipeline.jars",
        CONNECTOR_JAR.as_uri(),
    )

    settings = (
        EnvironmentSettings.new_instance()
        .with_configuration(config)
        .in_streaming_mode()
        .build()
    )

    table_env = TableEnvironment.create(settings)

    return table_env


# ---------------------------------------------------------------------
# FLIGHT STREAM PROCESSING JOB
# ---------------------------------------------------------------------

def run_flight_processor() -> None:
    """Submit the continuous Kafka-to-Kafka Flink analytics job."""

    logger.info("=" * 70)
    logger.info("AEROPULSE — REAL-TIME FLIGHT ANALYTICS")
    logger.info("=" * 70)

    logger.info("Flink connector: %s", CONNECTOR_JAR)
    logger.info("Kafka broker: %s", KAFKA_BROKER)
    logger.info("Input topic: %s", KAFKA_TOPIC_FLIGHTS)
    logger.info("Output topic: %s", KAFKA_TOPIC_ENRICHED)

    logger.info("Building Flink TableEnvironment...")

    table_env = build_table_environment()

    # Register source.
    logger.info("Registering Kafka flight source...")

    table_env.execute_sql(SOURCE_DDL)

    # Register sink.
    logger.info("Registering enriched Kafka sink...")

    table_env.execute_sql(SINK_DDL)

    # Submit the continuous transformation.
    logger.info("Submitting continuous Flink SQL transformation...")

    result = table_env.execute_sql(INSERT_SQL)

    logger.info("=" * 70)
    logger.info("FLINK JOB SUBMITTED")
    logger.info("=" * 70)

    logger.info("Input:  %s", KAFKA_TOPIC_FLIGHTS)
    logger.info("Output: %s", KAFKA_TOPIC_ENRICHED)
    logger.info("Flink Web UI: http://localhost:8081")

    logger.info(
        "Waiting for streaming job. Press Ctrl+C to stop."
    )

    # This blocks while the continuous streaming job is running.
    # We deliberately do not collect an unbounded stream into Python.
    result.wait()


# ---------------------------------------------------------------------
# APPLICATION ENTRY POINT
# ---------------------------------------------------------------------

if __name__ == "__main__":

    try:
        run_flight_processor()

    except KeyboardInterrupt:
        logger.warning(
            "AeroPulse Flink processor stopped by user."
        )

    except Exception:
        logger.exception(
            "AeroPulse Flink processing failed."
        )
        raise