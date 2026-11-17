"""Database inspection script for Assignment 4: Flight Streaming Analytics.

Displays flight telemetry summaries, phase distributions, and triggered alerts.
"""

from config import POSTGRES_CONFIG
import psycopg2

conn = psycopg2.connect(**POSTGRES_CONFIG)
cur = conn.cursor()

print("\n" + "=" * 80)
print("✈️   POSTGRESQL FLIGHT RADAR & TELEMETRY SUMMARY")
print("=" * 80)

# 1. Total Counts & Phase Distribution
cur.execute(
    "SELECT flight_phase, COUNT(*) FROM flight_telemetry GROUP BY flight_phase"
    " ORDER BY count DESC;"
)
phases = cur.fetchall()

print("\n📊 Active Aircraft by Flight Phase:")
for phase, count in phases:
  icon = (
      "🛫"
      if phase == "CLIMBING"
      else ("✈️" if phase == "CRUISING" else ("🛬" if phase == "DESCENDING" else "🅿️"))
  )
  print(f"   {icon} {phase:<15} : {count} aircraft")

# 2. Sample Live Flights in Telemetry Table
cur.execute("""
    SELECT flight_iata, airline_name, dep_iata, arr_iata, altitude_m, speed_kmh, flight_phase, delay_status, dep_delay
    FROM flight_telemetry 
    ORDER BY altitude_m DESC 
    LIMIT 10;
""")
sample_flights = cur.fetchall()

print("\n🌍 Top 10 Highest Cruising Flights (Live in Database):")
print("-" * 80)
print(
    f"{'FLIGHT':<10} | {'AIRLINE':<22} | {'ROUTE':<11} | {'ALTITUDE':<10} |"
    f" {'SPEED':<10} | {'PHASE':<10} | {'DELAY'}"
)
print("-" * 80)
for f in sample_flights:
  route = f"{f[2]} ➔ {f[3]}"
  alt = f"{f[4]:.0f} m"
  spd = f"{f[5]:.0f} km/h"
  delay = f"{f[7]} (+{f[8]}m)"
  print(
      f"{f[0]:<10} | {f[1][:22]:<22} | {route:<11} | {alt:<10} | {spd:<10} |"
      f" {f[6]:<10} | {delay}"
  )

# 3. Triggered Flight Alerts
cur.execute("""
    SELECT id, flight_iata, airline_name, alert_type, alert_message, created_at 
    FROM flight_alerts 
    ORDER BY created_at DESC 
    LIMIT 5;
""")
alerts = cur.fetchall()

print("\n" + "=" * 80)
print(f"🚨 TRIGGERED SAFETY & DELAY ALERTS (Total Logged: {len(alerts)})")
print("=" * 80)
if alerts:
  for a in alerts:
    print(f"  [Alert #{a[0]}] {a[1]} ({a[2]}): {a[4]}")
else:
  print("  No major alerts logged yet.")

print("=" * 80 + "\n")

cur.close()
conn.close()
