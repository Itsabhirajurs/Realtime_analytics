"""Canonical flight-event schema and Aviationstack normalization."""

import hashlib
import math
from datetime import datetime, timezone
from typing import Any


def _mapping(value: Any) -> dict:
    """Return a dictionary or an empty dictionary."""
    return value if isinstance(value, dict) else {}


def _text(value: Any, uppercase: bool = False) -> str | None:
    """Normalize optional text values."""
    if value is None:
        return None

    result = str(value).strip()

    if not result or result.lower() in {
        "none",
        "null",
        "n/a",
        "empty",
    }:
        return None

    return result.upper() if uppercase else result


def _number(value: Any) -> float | None:
    """Convert a value to a finite float."""
    if value is None or value == "":
        return None

    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (TypeError, ValueError):
        return None


def _integer(value: Any) -> int | None:
    """Convert a numeric value to an integer."""
    result = _number(value)
    return int(result) if result is not None else None


def _boolean(value: Any) -> bool | None:
    """Normalize optional Boolean values."""
    if isinstance(value, bool):
        return value

    if isinstance(value, str):
        normalized = value.strip().lower()

        if normalized in {"true", "1", "yes"}:
            return True

        if normalized in {"false", "0", "no"}:
            return False

    return None


def _utc_timestamp(value: Any) -> str | None:
    """Convert a timestamp to UTC ISO-8601."""
    if not value:
        return None

    try:
        parsed = datetime.fromisoformat(
            str(value).replace("Z", "+00:00")
        )

        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)

        return (
            parsed.astimezone(timezone.utc)
            .isoformat(timespec="seconds")
            .replace("+00:00", "Z")
        )

    except (TypeError, ValueError):
        return None


def _now_utc() -> str:
    """Return the current UTC timestamp."""
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def normalize_flight(
    raw: dict,
    data_mode: str,
    snapshot_fetched_at: str | None = None,
) -> dict:
    """Normalize one nested Aviationstack record into a flat event."""

    if not isinstance(raw, dict):
        raise ValueError("Flight record must be a JSON object.")

    flight = _mapping(raw.get("flight"))
    airline = _mapping(raw.get("airline"))
    aircraft = _mapping(raw.get("aircraft"))
    departure = _mapping(raw.get("departure"))
    arrival = _mapping(raw.get("arrival"))
    live = _mapping(raw.get("live"))

    # Flight and airline identifiers
    flight_iata = _text(flight.get("iata"), uppercase=True)
    flight_icao = _text(flight.get("icao"), uppercase=True)
    flight_number = _text(flight.get("number"))

    airline_name = _text(airline.get("name"))
    airline_iata = _text(airline.get("iata"), uppercase=True)
    airline_icao = _text(airline.get("icao"), uppercase=True)

    # Aircraft identifiers
    icao24 = _text(aircraft.get("icao24"), uppercase=True)
    registration = _text(
        aircraft.get("registration"),
        uppercase=True,
    )

    # Airport identifiers
    dep_iata = _text(departure.get("iata"), uppercase=True)
    arr_iata = _text(arrival.get("iata"), uppercase=True)

    flight_date = _text(raw.get("flight_date"))

    # A flight may lack an IATA callsign but still have an aircraft ID.
    flight_key = (
        icao24
        or flight_iata
        or flight_icao
        or "|".join(
            filter(
                None,
                [
                    flight_number,
                    flight_date,
                    dep_iata,
                    arr_iata,
                ],
            )
        )
    )

    if not flight_key:
        raise ValueError(
            "Record has no usable flight or aircraft identifier."
        )

    # Flight identity is distinct from physical aircraft identity.
    # This matters when codeshare records use the same ICAO24.
    flight_identity = (
        flight_iata
        or flight_icao
        or (
            f"{airline_iata}{flight_number}"
            if airline_iata and flight_number
            else None
        )
        or flight_key
    )

    # Telemetry
    latitude = _number(live.get("latitude"))
    longitude = _number(live.get("longitude"))

    has_position = (
        latitude is not None
        and longitude is not None
    )

    altitude_m = _number(live.get("altitude"))
    speed_kmh = _number(live.get("speed_horizontal"))
    vertical_rate = _number(live.get("speed_vertical"))

    heading_degrees = _number(live.get("direction"))
    is_ground = _boolean(live.get("is_ground"))

    # Source timestamps must not be replaced with ingestion time.
    source_observed_at = _utc_timestamp(
        live.get("updated")
    )

    ingested_at = _now_utc()

    # Schedule timestamps
    scheduled_departure = _utc_timestamp(
        departure.get("scheduled")
    )
    scheduled_arrival = _utc_timestamp(
        arrival.get("scheduled")
    )

    # Stable event identity:
    # Same flight snapshot -> same ID.
    # Different codeshare flight records -> different IDs.
    event_basis = "|".join(
        [
            "aviationstack",
            flight_identity,
            flight_icao or "",
            airline_iata or "",
            icao24 or "",
            flight_date or "",
            dep_iata or "",
            arr_iata or "",
            source_observed_at
            or scheduled_departure
            or scheduled_arrival
            or "unknown-time",
        ]
    )

    event_id = hashlib.sha256(
        event_basis.encode("utf-8")
    ).hexdigest()

    # Data-quality flags
    quality_flags = []

    if not live:
        quality_flags.append("NO_LIVE_TELEMETRY")

    if not has_position:
        quality_flags.append("NO_COMPLETE_POSITION")

    if not flight_iata:
        quality_flags.append("MISSING_FLIGHT_IATA")

    if not source_observed_at:
        quality_flags.append(
            "MISSING_SOURCE_OBSERVATION_TIME"
        )

    if not airline_name:
        quality_flags.append("MISSING_AIRLINE_NAME")

    return {
        # Event identity and provenance
        "event_id": event_id,
        "flight_identity": flight_identity,
        "flight_key": flight_key,
        "source": "aviationstack",
        "data_mode": data_mode,
        "is_cache_replay": data_mode == "cache_replay",
        "is_fresh_api_snapshot": data_mode == "api_snapshot",
        "source_snapshot_at": snapshot_fetched_at,
        "source_observed_at": source_observed_at,
        "ingested_at": ingested_at,

        # Compatibility fields
        "timestamp": source_observed_at,
        "flight_iata": flight_iata,
        "flight_number": flight_number,
        "flight_icao": flight_icao,
        "airline_name": airline_name or "Unknown Airline",
        "airline_iata": airline_iata,
        "airline_icao": airline_icao,
        "flight_status": (
            _text(raw.get("flight_status")) or "unknown"
        ),

        # Route
        "dep_airport": _text(departure.get("airport")),
        "dep_iata": dep_iata,
        "arr_airport": _text(arrival.get("airport")),
        "arr_iata": arr_iata,

        # Delays: null remains null
        "dep_delay": _integer(departure.get("delay")),
        "arr_delay": _integer(arrival.get("delay")),

        # Telemetry
        "latitude": latitude,
        "longitude": longitude,
        "altitude": altitude_m,
        "speed_horizontal": speed_kmh,
        "speed_vertical": vertical_rate,
        "is_ground": is_ground,

        # Additional aircraft information
        "icao24": icao24,
        "registration": registration,
        "aircraft_type": _text(aircraft.get("iata")),
        "flight_date": flight_date,

        # Explicit analytical units
        "altitude_m": altitude_m,
        "speed_kmh": speed_kmh,
        "vertical_rate_source_units": vertical_rate,
        "heading_degrees": heading_degrees,

        # Position quality
        "has_position": has_position,
        "source_observation_time_known": (
            source_observed_at is not None
        ),

        # Departure operations
        "dep_terminal": _text(departure.get("terminal")),
        "dep_gate": _text(departure.get("gate")),
        "dep_scheduled": scheduled_departure,
        "dep_estimated": _utc_timestamp(
            departure.get("estimated")
        ),
        "dep_actual": _utc_timestamp(
            departure.get("actual")
        ),
        "dep_actual_runway": _utc_timestamp(
            departure.get("actual_runway")
        ),

        # Arrival operations
        "arr_terminal": _text(arrival.get("terminal")),
        "arr_gate": _text(arrival.get("gate")),
        "arr_baggage": _text(arrival.get("baggage")),
        "arr_scheduled": scheduled_arrival,
        "arr_estimated": _utc_timestamp(
            arrival.get("estimated")
        ),
        "arr_actual": _utc_timestamp(
            arrival.get("actual")
        ),
        "arr_actual_runway": _utc_timestamp(
            arrival.get("actual_runway")
        ),

        # Data-quality diagnostics
        "quality_flags": quality_flags,
    }