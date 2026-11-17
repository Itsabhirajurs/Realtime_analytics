import psycopg2


conn = psycopg2.connect(
    dbname="phyphox_db",
    user="postgres",
    password="admin123",
    host="localhost",
    port="5432",
)
cur = conn.cursor()

cur.execute(
    "SELECT candidate_id, name, dob, age, processed_at FROM candidate_profiles"
    " ORDER BY candidate_id;"
)
rows = cur.fetchall()

print("\n" + "=" * 75)
print("📊 Current Records in PostgreSQL ('candidate_profiles' Table)")
print("=" * 75)
print(
    f"{'CANDIDATE ID':<15} | {'NAME':<18} | {'DOB':<12} | {'AGE':<5} |"
    " {'PROCESSED AT'}"
)
print("-" * 75)
for r in rows:
  print(f"{r[0]:<15} | {r[1]:<18} | {str(r[2]):<12} | {r[3]:<5} | {r[4]}")
print("=" * 75 + "\n")

cur.close()
conn.close()
