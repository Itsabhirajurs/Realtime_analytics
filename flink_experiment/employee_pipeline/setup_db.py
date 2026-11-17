import psycopg2

print("Connecting to PostgreSQL to set up candidate_profiles table...")

# Connect using your existing PostgreSQL credentials
conn = psycopg2.connect(
    dbname="phyphox_db",
    user="postgres",
    password="admin123",
    host="localhost",
    port="5432",
)
conn.autocommit = True
cur = conn.cursor()

# Create table with candidate_id, name, dob, calculated age, and timestamp
create_table_query = """
CREATE TABLE IF NOT EXISTS candidate_profiles (
    candidate_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(100),
    dob DATE,
    age INT,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

cur.execute(create_table_query)
print("Table 'candidate_profiles' is ready in PostgreSQL!")

cur.close()
conn.close()
