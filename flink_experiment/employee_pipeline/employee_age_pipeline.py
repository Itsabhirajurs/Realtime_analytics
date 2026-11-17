from datetime import datetime
import psycopg2
from pyflink.common import Row
from pyflink.table import DataTypes, EnvironmentSettings, TableEnvironment


def run_employee_pipeline():
  print("=" * 60)
  print("🚀 Starting PyFlink Table API Employee Age Pipeline")
  print("=" * 60)

  # 1. Initialize Flink Streaming Table Environment
  env_settings = EnvironmentSettings.in_streaming_mode()
  t_env = TableEnvironment.create(env_settings)
  t_env.get_config().set("parallelism.default", "1")

  # 2. Raw Candidate Data (Simulating an incoming applicant stream)
  raw_candidates = [
      ("CAND_101", "Aarav Sharma", "1995-04-12"),
      ("CAND_102", "Priya Patel", "2001-11-23"),
      ("CAND_103", "Rahul Verma", "1988-08-05"),
      ("CAND_104", "Sneha Rao", "2003-02-17"),
      ("CAND_105", "Vikram Singh", "1992-09-30"),
      ("CAND_106", "Ananya Iyer", "1999-12-01"),
  ]

  # 3. Define the Schema for the Ingested Table
  schema = DataTypes.ROW([
      DataTypes.FIELD("candidate_id", DataTypes.STRING()),
      DataTypes.FIELD("name", DataTypes.STRING()),
      DataTypes.FIELD("dob", DataTypes.STRING()),
  ])

  # Ingest into Flink Table
  source_table = t_env.from_elements(raw_candidates, schema)
  t_env.create_temporary_view("candidate_stream", source_table)
  print("📥 Raw candidate records ingested into Flink Table.")

  # 4. Flink SQL Transformation: Compute Age On-the-Fly
  # TIMESTAMPDIFF(YEAR, TO_DATE(dob, 'yyyy-MM-dd'), CURRENT_DATE)
  query = """
    SELECT 
        candidate_id,
        name,
        TO_DATE(dob, 'yyyy-MM-dd') AS dob_date,
        TIMESTAMPDIFF(YEAR, TO_DATE(dob, 'yyyy-MM-dd'), CURRENT_DATE) AS calculated_age
    FROM candidate_stream
    """

  result_table = t_env.sql_query(query)
  print("⚙️  Flink Table transformation query compiled.")

  # 5. Connect to PostgreSQL
  print("🔌 Connecting to PostgreSQL...")
  db_conn = psycopg2.connect(
      dbname="phyphox_db",
      user="postgres",
      password="admin123",
      host="localhost",
      port="5432",
  )
  db_conn.autocommit = True
  cur = db_conn.cursor()

  # 6. Stream transformed records from Flink Table and sink to PostgreSQL
  print("📤 Streaming transformed rows from Flink into PostgreSQL...\n")

  insert_sql = """
    INSERT INTO candidate_profiles (candidate_id, name, dob, age)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (candidate_id) 
    DO UPDATE SET name = EXCLUDED.name, dob = EXCLUDED.dob, age = EXCLUDED.age;
    """

  # execute().collect() pulls each row as Flink processes it
  with result_table.execute().collect() as results:
    for row in results:
      c_id = row[0]
      c_name = row[1]
      c_dob = row[2]  # PyFlink Date object
      c_age = row[3]  # Flink-computed age

      print(
          f"  [FLINK ENGINE] Processed: {c_id} ({c_name}) | DOB: {c_dob} |"
          f" Calculated Age: {c_age}"
      )

      cur.execute(insert_sql, (c_id, c_name, str(c_dob), int(c_age)))

  print("\n All records successfully transformed by Flink and saved in Postgres!")
  cur.close()
  db_conn.close()


if __name__ == "__main__":
  run_employee_pipeline()
