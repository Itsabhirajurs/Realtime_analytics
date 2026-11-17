"""AeroPulse flight ingestion and Kafka publishing service.

Default mode: replay the local cache.
Live API access requires an explicit --live flag.
"""

import argparse
import json
import logging
import time

from datetime import datetime, timezone
from pathlib import Path

import requests

from kafka import KafkaProducer

from config import (
    AVIATIONSTACK_KEY,
    AVIATIONSTACK_URL,
    API_TIMEOUT_SECONDS,
    CACHE_FILE,
    KAFKA_BROKER,
    KAFKA_CLIENT_ID,
    KAFKA_REQUEST_TIMEOUT_MS,
    KAFKA_TOPIC_FLIGHTS,
    MAX_FLIGHT_BATCH_SIZE,
)

from flight_schema import normalize_flight


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger("aeropulse.producer")


def utc_now() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def validate_payload(payload: dict) -> list:
    """Validate the minimum expected Aviationstack response structure."""

    if not isinstance(payload, dict):
        raise ValueError("API/cache response must be a JSON object.")

    if isinstance(payload.get("error"), dict):
        message = payload["error"].get("message", "Unknown API error")
        raise RuntimeError(f"Aviationstack API error: {message}")

    records = payload.get("data")

    if not isinstance(records, list):
        raise ValueError("Expected a list under response key 'data'.")

    return records


def save_cache(payload: dict) -> None:
    """Atomically save an API snapshot without damaging the old cache."""

    cache_path = Path(CACHE_FILE)
    cache_path.parent.mkdir(parents=True, exist_ok=True)

    saved_payload = dict(payload)

    saved_payload["_aeropulse_cache"] = {
        "source": "aviationstack",
        "fetched_at_utc": utc_now(),
        "record_count": len(saved_payload.get("data", [])),
        "cache_version": 1,
    }

    temporary_path = cache_path.with_name(
        cache_path.name + ".tmp"
    )

    with temporary_path.open("w", encoding="utf-8") as file:
        json.dump(
            saved_payload,
            file,
            indent=2,
            ensure_ascii=False,
        )

    temporary_path.replace(cache_path)

    logger.info("Saved API snapshot to %s", cache_path)


def load_cache() -> tuple[list, str | None]:
    """Load cache without making any API request."""

    cache_path = Path(CACHE_FILE)

    if not cache_path.exists():
        raise FileNotFoundError(
            f"Cache not found: {cache_path}. "
            "No API request was made. "
            "Use --live explicitly only when you intend to spend an API credit."
        )

    with cache_path.open("r", encoding="utf-8") as file:
        payload = json.load(file)

    records = validate_payload(payload)

    cache_metadata = payload.get("_aeropulse_cache") or {}
    snapshot_time = cache_metadata.get("fetched_at_utc")

    logger.info(
        "Loaded %d cached records. Source snapshot time: %s",
        len(records),
        snapshot_time or "unknown",
    )

    return records, snapshot_time


def fetch_live_snapshot(limit: int) -> tuple[list, str]:
    """Make exactly one explicit API request; no automatic retries."""

    if not AVIATIONSTACK_KEY:
        raise RuntimeError(
            "AVIATIONSTACK_KEY is missing from your local .env file."
        )

    logger.warning(
        "LIVE API REQUEST: this operation uses one Aviationstack request."
    )

    params = {
        "access_key": AVIATIONSTACK_KEY,
        "flight_status": "active",
        "limit": limit,
    }

    response = requests.get(
        AVIATIONSTACK_URL,
        params=params,
        timeout=API_TIMEOUT_SECONDS,
    )

    response.raise_for_status()

    payload = response.json()
    records = validate_payload(payload)

    fetched_at = utc_now()

    # Keep the raw API response intact, with separate cache metadata.
    save_cache(payload)

    logger.info(
        "Received %d flight records from Aviationstack.",
        len(records),
    )

    return records, fetched_at


def normalize_batch(
    raw_records: list,
    data_mode: str,
    snapshot_time: str | None,
) -> tuple[list, int]:
    """Normalize records and count rejected entries."""

    normalized = []
    rejected = 0

    for index, raw in enumerate(raw_records):
        try:
            event = normalize_flight(
                raw=raw,
                data_mode=data_mode,
                snapshot_fetched_at=snapshot_time,
            )

            normalized.append(event)

        except (TypeError, ValueError) as exc:
            rejected += 1
            logger.warning(
                "Rejected source record %d: %s",
                index,
                exc,
            )

    return normalized, rejected


def kafka_producer() -> KafkaProducer:
    """Create a reliable Kafka producer."""

    brokers = [
        address.strip()
        for address in KAFKA_BROKER.split(",")
        if address.strip()
    ]

    if not brokers:
        raise ValueError("No Kafka bootstrap server configured.")

    return KafkaProducer(
        bootstrap_servers=brokers,
        client_id=KAFKA_CLIENT_ID,
        acks="all",
        retries=5,
        max_in_flight_requests_per_connection=1,
        request_timeout_ms=KAFKA_REQUEST_TIMEOUT_MS,
        delivery_timeout_ms=120000,
        compression_type="gzip",
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8"),
    )


def publish_to_kafka(
    records: list,
    delay_seconds: float = 0.0,
) -> int:
    """Publish records and wait for broker acknowledgements."""

    if not records:
        logger.warning("No valid records to publish.")
        return 0

    logger.info(
        "Connecting to Kafka: %s | Topic: %s",
        KAFKA_BROKER,
        KAFKA_TOPIC_FLIGHTS,
    )

    producer = kafka_producer()
    acknowledged = 0

    try:
        for index, event in enumerate(records, start=1):
            future = producer.send(
                KAFKA_TOPIC_FLIGHTS,
                key=event["flight_key"],
                value=event,
            )

            metadata = future.get(
                timeout=(KAFKA_REQUEST_TIMEOUT_MS / 1000) + 10
            )

            acknowledged += 1

            logger.info(
                "[%d/%d] %s | %s -> %s | "
                "position=%s | partition=%d offset=%d",
                index,
                len(records),
                event.get("flight_iata")
                or event.get("flight_key"),
                event.get("dep_iata") or "UNKNOWN",
                event.get("arr_iata") or "UNKNOWN",
                event.get("has_position"),
                metadata.partition,
                metadata.offset,
            )

            if delay_seconds > 0:
                time.sleep(delay_seconds)

        producer.flush(timeout=30)

    finally:
        producer.close(timeout=30)

    logger.info(
        "Publishing finished: %d/%d records acknowledged.",
        acknowledged,
        len(records),
    )

    return acknowledged


def main() -> None:
    parser = argparse.ArgumentParser(
        description="AeroPulse flight telemetry ingestion service"
    )

    parser.add_argument(
        "--live",
        action="store_true",
        help="Fetch one fresh Aviationstack snapshot (uses one API request).",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Normalize cached records without publishing to Kafka.",
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=MAX_FLIGHT_BATCH_SIZE,
        help="Maximum number of records requested from Aviationstack.",
    )

    parser.add_argument(
        "--max-records",
        type=int,
        default=None,
        help="Optional limit on the number of records published.",
    )

    parser.add_argument(
        "--delay",
        type=float,
        default=0.0,
        help="Delay between Kafka messages in seconds.",
    )

    args = parser.parse_args()

    if args.live and args.dry_run:
        parser.error(
            "--live cannot be combined with --dry-run. "
            "A live request should not be used for a dry run."
        )

    if not 1 <= args.limit <= 100:
        parser.error("--limit must be between 1 and 100.")

    if args.max_records is not None and args.max_records < 1:
        parser.error("--max-records must be at least 1.")

    if args.delay < 0:
        parser.error("--delay cannot be negative.")

    # Source selection is explicit and safe.
    if args.live:
        raw_records, snapshot_time = fetch_live_snapshot(args.limit)
        data_mode = "api_snapshot"
    else:
        raw_records, snapshot_time = load_cache()
        data_mode = "cache_replay"

    records, rejected = normalize_batch(
        raw_records,
        data_mode,
        snapshot_time,
    )

    if args.max_records:
        records = records[:args.max_records]

    positions = sum(
        1 for event in records if event["has_position"]
    )

    logger.info("=" * 65)
    logger.info("AEROPULSE INGESTION SUMMARY")
    logger.info("Data mode: %s", data_mode)
    logger.info("Source records: %d", len(raw_records))
    logger.info("Valid records: %d", len(records))
    logger.info("Rejected records: %d", rejected)
    logger.info("Records with coordinates: %d", positions)
    logger.info("Records without coordinates: %d", len(records) - positions)
    logger.info("=" * 65)

    if args.dry_run:
        logger.info("DRY RUN: Kafka was not contacted.")

        for event in records[:5]:
            logger.info(
                "Preview: %s | %s -> %s | "
                "mode=%s | position=%s",
                event.get("flight_iata") or event["flight_key"],
                event.get("dep_iata"),
                event.get("arr_iata"),
                event["data_mode"],
                event["has_position"],
            )

        return

    publish_to_kafka(
        records,
        delay_seconds=args.delay,
    )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("Producer stopped by user.")
    except Exception as exc:
        logger.exception("Flight ingestion failed: %s", exc)
        raise