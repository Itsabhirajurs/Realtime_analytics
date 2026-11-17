import psycopg2
import pandas as pd

print("Connecting to PostgreSQL...")
try:
    db_connection = psycopg2.connect(
        dbname="phyphox_db",
        user="postgres",
        password="admin123",
        host="localhost",
        port="5432"
    )
    cursor = db_connection.cursor()
    
    # Manually fetch data to avoid the Pandas warning
    cursor.execute("SELECT record_time, acc_x, acc_y, acc_z, magnitude FROM accelerometer ORDER BY record_time;")
    records = cursor.fetchall()
    
    # Convert to DataFrame
    df = pd.DataFrame(records, columns=['record_time', 'acc_x', 'acc_y', 'acc_z', 'magnitude'])
    
    csv_filename = "kafka_sensor_data.csv"
    df.to_csv(csv_filename, index=False)
    
    print(f"Success! {len(df)} rows exported to {csv_filename}")
    
except Exception as e:
    print(f"Error: {e}")
finally:
    if 'db_connection' in locals():
        cursor.close()
        db_connection.close()