# 🌦️ Real-Time Weather & Agronomic Streaming Platform
### Apache Kafka • Dual-Consumer Microservices • PostgreSQL • Apache Superset

---

## 1. Executive Summary & Project Overview
This project implements an end-to-end, event-driven streaming platform designed to ingest, process, and visualize global weather telemetry in real time. 

Built around a **decoupled pub/sub streaming architecture**, the platform addresses two distinct business personas with completely independent analytical needs:
1. **General Public Health Persona**: Monitors ambient atmospheric metrics, converts temperature scales (°C / °F), and raises immediate **Critical Heat Warnings** when temperatures exceed **32°C (89.6°F)**.
2. **Agricultural / Farmer Decision Persona**: Ingests multi-day weather forecasts, aggregates daily trends, triggers **High Humidity Pathogen Alerts (>80%)** to prevent crop blight, and computes agricultural **Chemical Spraying Windows**.

The visualization layer is powered by **Apache Superset (via Docker)**, presenting real-time KPI cards, time-series curves, risk distribution donuts, and agronomic decision matrices.

---

## 2. End-to-End System Architecture