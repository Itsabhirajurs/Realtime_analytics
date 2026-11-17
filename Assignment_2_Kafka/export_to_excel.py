import psycopg2
import pandas as pd

print("Connecting to PostgreSQL...")
try:
    # Connect to the database
    db_connection = psycopg2.connect(
        dbname="phyphox_db",
        user="postgres",
        password="admin123",
        host="localhost",
        port="5432"
    )
    
    # Read the entire table directly into a Pandas DataFrame
    query = "SELECT record_time, acc_x, acc_y, acc_z, magnitude FROM accelerometer ORDER BY record_time;"
    df = pd.read_sql_query(query, db_connection)
    
    # Export it to a CSV file
    excel_filename = "kafka_sensor_data.xlsx"
    df.to_excel(excel_filename, index=False)
    
    print(f"Success! {len(df)} rows exported to {excel_filename}")
    
except Exception as e:
    print(f"Error: {e}")
finally:
    if 'db_connection' in locals():     
        db_connection.close()
        