# ✈️ Assignment 4: Real-Time Flight Analytics Pipeline

## 1. Project Overview & Objective
This project builds an end-to-end real-time aviation telemetry pipeline:
- Ingests global flight data from the **Aviationstack API (APILayer)**.
- Streams live telemetry events into **Apache Kafka** (running in WSL on `172.17.204.151:9092`).
- Uses **Apache Flink (PyFlink 2.3)** to compute real-time analytics (flight phase detection, delay flagging, speed & altitude metrics).
- Persists enriched flight intelligence into **PostgreSQL** (`phyphox_db` on `localhost:5432`).

---

## 2. API Quota Strategy (Aviationstack Free Tier)
- **Constraint**: The free tier provides **100 total API requests**.
- **Strategy**: 
  - Each API request fetches up to 100 active flights (`limit=100`).
  - We implement a **Smart Replay Cache** (`flight_cache.json`).
  - The producer can replay cached flights as a continuous real-time stream for testing without consuming API credits.
  - Live fetching is enabled on demand for real-time demonstrations.

---

## 3. End-to-End System Architecture



---

## 4. Key Metrics & Transformations in Flink
1. **Flight Phase Detection**:
   - `On Ground`: `live.is_ground == True`
   - `Climbing`: `speed_vertical > 2.0 m/s` and `altitude < 8000m`
   - `Cruising`: `altitude >= 8000m` and `abs(speed_vertical) <= 2.0 m/s`
   - `Descending / Approach`: `speed_vertical < -2.0 m/s` and `altitude < 8000m`
2. **Delay Alerting**:
   - Departure or arrival delay > 30 minutes triggers a `DELAY_WARNING`.
3. **Speed & Fleet Aggregation**:
   - Average speed and active aircraft counts grouped by airline.

---

## 5. File Registry
| File | Role |
| :--- | :--- |
| `PROJECT_LOG.md` | Master project log, architecture, and developer notes |
| `config.py` | API keys, Kafka broker, PostgreSQL credentials, and thresholds |
| `flight_producer.py` | Fetches/caches Aviationstack data and streams JSON to Kafka |
| `flight_flink_processor.py` | PyFlink Table API job computing flight metrics |
| `setup_flight_db.py` | Sets up PostgreSQL tables for flight telemetry and alerts |
