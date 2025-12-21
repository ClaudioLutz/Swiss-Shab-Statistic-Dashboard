
import os
# Fix for PyArrow/Pandas compatibility issue - must be set before importing pandas
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"

import json
import logging
from flask import Flask, render_template, jsonify, send_from_directory, url_for
import pandas as pd
from parquet_utils import safe_read_parquet
from logging_setup import configure_logging

from logging_setup import configure_logging

logger = logging.getLogger(__name__)

app = Flask(__name__)

SHAB_DATA_DIR = './shab_data'
UDEMO_MERGED_FILE = os.path.join(SHAB_DATA_DIR, 'udemo_merged.parquet')
STATIC_FOLDER = './static'

@app.route("/")
def home():
    # Check if dashboard data exists
    data_path = os.path.join(STATIC_FOLDER, 'data', 'shab_monthly.json')
    
    if os.path.exists(data_path):
        return render_template('dashboard.html')
    
    # Fallback/Legacy check
    facet_plot = os.path.join(STATIC_FOLDER, 'FacetGridKanton.png')
    if os.path.exists(facet_plot):
         return render_template('visualisation.html')
    
    return render_template('loading.html', message="Data not generated yet. Please run 'python refresh_data.py' in the console.")

@app.get("/api/status")
def api_status():
    status_path = os.path.join(app.static_folder, "status.json")
    if not os.path.exists(status_path):
        return jsonify({"state": "missing", "message": "status.json not found. Run refresh_data.py."}), 404

    with open(status_path, "r", encoding="utf-8") as f:
        return jsonify(json.load(f))

@app.route("/api/udemo_vs_shab")
def udemo_vs_shab():
    if not os.path.exists(UDEMO_MERGED_FILE):
        return jsonify({"error": "Data not ready"}), 503
    
    try:
        df = safe_read_parquet(UDEMO_MERGED_FILE)
        if df is None:
             return jsonify({"error": "Failed to read data"}), 500

        # Replace NaN with null (None) for JSON compatibility
        records = df.where(pd.notnull(df), None).to_dict(orient="records")
        return jsonify(records)
    except Exception as e:
        logger.error(f"Error reading merged data: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/progress")
def progress():
    # Check if data is ready by verifying plot files exist
    facet_plot = os.path.join(STATIC_FOLDER, 'FacetGridKanton.png')
    line_plot = os.path.join(STATIC_FOLDER, 'LineGraph.png')
    
    data_ready = os.path.exists(facet_plot) and os.path.exists(line_plot)
    
    if data_ready:
        return jsonify({
            'status': 'complete',
            'message': 'Ready',
            'current': 1,
            'total': 1
        })
    else:
        return jsonify({
            'status': 'missing',
            'message': 'Run refresh_data.py',
            'current': 0,
            'total': 1
        })

if __name__ == "__main__":
    configure_logging(level="INFO", log_file="flask.log")
    app.run(debug=True)
