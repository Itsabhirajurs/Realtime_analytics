"""Unit tests for the AeroPulse flight event schema."""

import unittest

from flight_schema import normalize_flight


class FlightSchemaTests(unittest.TestCase):

    def test_missing_live_data_is_preserved_as_null(self):
        raw = {
            "flight_date": "2026-10-24",
            "flight": {
                "iata": "FD4110",
                "number": "4110",
            },
            "airline": {
                "name": "AirAsia",
            },
            "departure": {
                "iata": "BKK",
                "delay": None,
            },
            "arrival": {
                "iata": "HKT",
                "delay": None,
            },
            "live": None,
        }

        event = normalize_flight(raw, "cache_replay")

        self.assertEqual(event["flight_iata"], "FD4110")
        self.assertIsNone(event["latitude"])
        self.assertIsNone(event["longitude"])
        self.assertIsNone(event["dep_delay"])
        self.assertIsNone(event["arr_delay"])

        self.assertFalse(event["has_position"])
        self.assertTrue(event["is_cache_replay"])
        self.assertFalse(event["is_fresh_api_snapshot"])

    def test_live_telemetry_is_normalized(self):
        raw = {
            "flight_date": "2026-10-24",
            "flight": {
                "iata": "FS7315",
                "number": "7315",
            },
            "airline": {
                "name": "FlyArystan",
                "iata": "FS",
            },
            "aircraft": {
                "icao24": "4CABFE",
                "registration": "EI-KBE",
                "iata": "A320",
            },
            "departure": {
                "iata": "NQZ",
                "delay": 26,
            },
            "arrival": {
                "iata": "KZO",
                "delay": 6,
            },
            "live": {
                "updated": "2026-10-24T05:34:17+00:00",
                "latitude": 50.0924,
                "longitude": 70.341,
                "altitude": 8465.82,
                "speed_horizontal": 811.176,
                "speed_vertical": 0,
                "is_ground": False,
                "direction": 216,
            },
        }

        event = normalize_flight(raw, "cache_replay")

        self.assertEqual(event["flight_key"], "4CABFE")
        self.assertEqual(event["flight_identity"], "FS7315")

        self.assertTrue(event["has_position"])
        self.assertEqual(event["latitude"], 50.0924)
        self.assertEqual(event["longitude"], 70.341)

        self.assertEqual(event["altitude_m"], 8465.82)
        self.assertEqual(event["speed_kmh"], 811.176)
        self.assertEqual(event["heading_degrees"], 216.0)

        self.assertFalse(event["is_ground"])
        self.assertTrue(event["source_observation_time_known"])

        self.assertEqual(event["aircraft_type"], "A320")
        self.assertEqual(event["registration"], "EI-KBE")

    def test_stable_event_id_for_same_snapshot(self):
        raw = {
            "flight_date": "2026-10-24",
            "flight": {
                "iata": "FD4110",
            },
            "departure": {
                "iata": "BKK",
                "scheduled": "2026-10-24T11:55:00+00:00",
            },
            "arrival": {
                "iata": "HKT",
                "scheduled": "2026-10-24T13:30:00+00:00",
            },
            "live": {
                "updated": "2026-10-24T05:34:17+00:00",
            },
        }

        first = normalize_flight(raw, "cache_replay")
        second = normalize_flight(raw, "cache_replay")

        self.assertEqual(first["event_id"], second["event_id"])

    def test_invalid_record_without_identifier_is_rejected(self):
        with self.assertRaises(ValueError):
            normalize_flight(
                {
                    "flight": {},
                    "aircraft": {},
                    "departure": {},
                    "arrival": {},
                },
                "cache_replay",
            )

    def test_codeshare_flights_have_distinct_event_ids(self):
        common = {
            "flight_date": "2026-10-24",
            "aircraft": {
                "icao24": "7809B6",
            },
            "departure": {
                "iata": "PKX",
                "scheduled": "2026-10-24T14:05:00+00:00",
            },
            "arrival": {
                "iata": "FOC",
                "scheduled": "2026-10-24T16:50:00+00:00",
            },
            "live": None,
        }

        klm = {
            **common,
            "flight": {
                "iata": "KL3763",
                "number": "3763",
                "icao": "KLM3763",
            },
            "airline": {
                "iata": "KL",
                "name": "KLM",
            },
        }

        hebei = {
            **common,
            "flight": {
                "iata": "NS8166",
                "number": "8166",
                "icao": "HBH8166",
            },
            "airline": {
                "iata": "NS",
                "name": "Hebei Airlines",
            },
        }

        first = normalize_flight(klm, "cache_replay")
        second = normalize_flight(hebei, "cache_replay")

        # Same aircraft, different flight identities.
        self.assertEqual(first["flight_key"], second["flight_key"])

        self.assertNotEqual(
            first["flight_identity"],
            second["flight_identity"],
        )

        self.assertNotEqual(
            first["event_id"],
            second["event_id"],
        )

    def test_empty_airline_name_is_normalized(self):
        raw = {
            "flight_date": "2026-10-24",
            "flight": {
                "iata": "TEST123",
            },
            "airline": {
                "name": "empty",
            },
            "departure": {},
            "arrival": {},
            "live": None,
        }

        event = normalize_flight(raw, "cache_replay")

        self.assertEqual(
            event["airline_name"],
            "Unknown Airline",
        )

        self.assertIn(
            "MISSING_AIRLINE_NAME",
            event["quality_flags"],
        )


if __name__ == "__main__":
    unittest.main()