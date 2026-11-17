from flask import Flask, render_template, jsonify
import requests
import numpy as np
import pandas as pd
from scipy.signal import find_peaks

app = Flask(__name__)

# --- CONFIGURATION ---
PHYPHOX_URL = "http://10.230.99.119:8080" 
STRIDE_LENGTH = 0.75 

@app.route('/')
def index():
    # This serves the HTML dashboard to your browser
    return render_template('index.html')

@app.route('/api/data')
def get_live_data():
    """This route fetches data from phyphox, processes it, and sends it to the frontend."""
    url = f"{PHYPHOX_URL}/get?accX=full&accY=full&accZ=full&acc_time=full"
    
    try:
        response = requests.get(url, timeout=2)
        if response.status_code != 200:
            return jsonify({"error": "Failed to connect to phyphox"}), 500
            
        data = response.json().get("buffer", {})
        t = data.get("acc_time", {}).get("buffer", [])
        x = data.get("accX", {}).get("buffer", [])
        y = data.get("accY", {}).get("buffer", [])
        z = data.get("accZ", {}).get("buffer", [])

        if not t:
            return jsonify({"error": "No data yet. Press Play in phyphox."}), 400

        # 1. Convert to Pandas & Calculate Magnitude
        df = pd.DataFrame({"time": t, "x": x, "y": y, "z": z})
        df["magnitude"] = np.sqrt(df["x"]**2 + df["y"]**2 + df["z"]**2)

        # 2. Step Detection Math
        adaptive_threshold = np.mean(df["magnitude"]) + 1.2
        peaks, _ = find_peaks(df["magnitude"], height=adaptive_threshold, distance=15)
        
        total_steps = len(peaks)
        distance = total_steps * STRIDE_LENGTH

        # 3. For the live chart, we only send the last 100 points so the browser doesn't lag
        tail_df = df.tail(100)
        
        return jsonify({
            "time": tail_df["time"].tolist(),
            "magnitude": tail_df["magnitude"].tolist(),
            "steps": total_steps,
            "distance": round(distance, 2)
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    # Run the server on port 5000
    app.run(debug=True, port=5000)