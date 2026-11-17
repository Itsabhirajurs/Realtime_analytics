import requests
import pandas as pd
import time
import os

# --- CONFIGURATION ---
PHYPHOX_URL = "http://10.230.99.119:8080" 
DURATION_MINUTES = 3
POLL_INTERVAL_SECONDS = 0.5 # How often we fetch from Phyphox
CSV_FILENAME = "accelerometer_data.csv"

def collect_data():
    print(f"Starting data collection from {PHYPHOX_URL} for {DURATION_MINUTES} minutes...")
    print(f"Please ensure Phyphox is open, 'Allow remote access' is ON, and you have started the 'Acceleration with g' or 'Raw Sensors -> Accelerometer' experiment.")
    print("Press play in the Phyphox app if you haven't already.")
    
    end_time = time.time() + (DURATION_MINUTES * 60)
    
    # Check if we can connect
    try:
        response = requests.get(f"{PHYPHOX_URL}/get?accX=full", timeout=5)
        if response.status_code != 200:
            print("Error: Could not connect to Phyphox. Is the experiment running and IP correct?")
            return
    except requests.exceptions.RequestException as e:
        print(f"Connection failed: {e}")
        print("Please check your IP address and ensure your phone and computer are on the same WiFi network.")
        return

    # Clear previous CSV file if it exists
    if os.path.exists(CSV_FILENAME):
        os.remove(CSV_FILENAME)
        
    last_time_seen = -1.0 # Keep track of the last time to avoid duplicates
    total_samples = 0
    
    try:
        while time.time() < end_time:
            # We request full data buffers. 
            url = f"{PHYPHOX_URL}/get?accX=full&accY=full&accZ=full&acc_time=full"
            
            try:
                response = requests.get(url)
                if response.status_code == 200:
                    data = response.json().get("buffer", {})
                    t = data.get("acc_time", {}).get("buffer", [])
                    x = data.get("accX", {}).get("buffer", [])
                    y = data.get("accY", {}).get("buffer", [])
                    z = data.get("accZ", {}).get("buffer", [])
                    
                    if t and len(t) > 0:
                        # Create a dataframe for the new data
                        df = pd.DataFrame({"Time(s)": t, "X(m/s^2)": x, "Y(m/s^2)": y, "Z(m/s^2)": z})
                        
                        # Filter only new data based on the last seen timestamp
                        new_df = df[df["Time(s)"] > last_time_seen]
                        
                        if not new_df.empty:
                            # Append to CSV
                            header = not os.path.exists(CSV_FILENAME)
                            new_df.to_csv(CSV_FILENAME, mode='a', index=False, header=header)
                            
                            last_time_seen = new_df["Time(s)"].iloc[-1]
                            total_samples += len(new_df)
                            
                            # Print progress
                            remaining = int(end_time - time.time())
                            print(f"Collected {total_samples} samples so far. Time remaining: {remaining}s   ", end='\r')
            except requests.exceptions.RequestException:
                pass # Ignore occasional timeout/connection drops during the loop
            
            time.sleep(POLL_INTERVAL_SECONDS)
            
    except KeyboardInterrupt:
        print("\nCollection stopped manually.")
    
    print(f"\nData collection finished! Total samples: {total_samples}")
    print(f"Saved to {os.path.abspath(CSV_FILENAME)}")

if __name__ == "__main__":
    collect_data()
