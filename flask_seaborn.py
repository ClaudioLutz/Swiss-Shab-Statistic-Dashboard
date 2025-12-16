import os
# Fix for PyArrow/Pandas compatibility issue - must be set before importing pandas
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"

from flask import Flask, send_file, render_template, jsonify
import io
import base64
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
import matplotlib.pyplot as plt
import seaborn as sns
from app import Get_Shab_DF_from_range, grouped_multiple, FacetGridKanton, grouped_multiple_ohne_Kantone, LineGraph
from datetime import date, time, timedelta, datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import logging
import threading

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Global variable to cache the data
_data_cache = None

# Global variable to track generation progress
_generation_status = {
    'state': 'idle',  # idle, processing, complete, error
    'current': 0,
    'total': 0,
    'message': ''
}

def update_progress(current, total, message):
    global _generation_status
    _generation_status['current'] = current
    _generation_status['total'] = total
    _generation_status['message'] = message
    # logger.info(f"Progress update: {current}/{total} - {message}")

def generate_data_task():
    global _data_cache, _generation_status
    
    try:
        _generation_status['state'] = 'processing'
        _generation_status['message'] = 'Starting data generation...'
        logger.info("Starting data generation process...")

        # Calculate date range
        given_date = datetime.today().date()
        end_date = given_date.replace(day=1) - timedelta(days=1)
        start_date = end_date - relativedelta(years=3)
        start_date = start_date + timedelta(days=1)

        logger.info(f"Fetching data from {start_date} to {end_date}")
        _generation_status['message'] = f"Fetching data from {start_date} to {end_date}"

        # Get data
        # Pass callback to Get_Shab_DF_from_range
        df = Get_Shab_DF_from_range(start_date, end_date, progress_callback=update_progress)
        logger.info(f"Successfully fetched {len(df)} records")

        # Process data
        _generation_status['message'] = "Processing data..."
        update_progress(100, 100, "Processing data - grouping by month, subrubric, and kanton...")
        logger.info("Processing data - grouping by month, subrubric, and kanton...")
        grouped_multiples = grouped_multiple(df)

        # Generate plots
        _generation_status['message'] = "Generating plots..."
        update_progress(100, 100, "Generating FacetGrid plot for Kantons...")
        logger.info("Generating FacetGrid plot for Kantons...")
        FacetGridKanton(grouped_multiples, start_date, end_date)
        logger.info("FacetGrid plot saved to ./static/FacetGridKanton.png")

        update_progress(100, 100, "Processing data without Kantons...")
        logger.info("Processing data without Kantons...")
        grouped_multiple_ohne_Kanton = grouped_multiple_ohne_Kantone(df)

        update_progress(100, 100, "Generating line graph...")
        logger.info("Generating line graph...")
        fig, ax = plt.subplots(figsize=(20,6))
        ax = sns.set_style(style='darkgrid')
        sns.lineplot(data=grouped_multiple_ohne_Kanton, x="month", y='count', hue='subrubric')
        plt.xticks(rotation=60)
        plt.savefig("./static/LineGraph.png")
        plt.close()
        logger.info("Line graph saved to ./static/LineGraph.png")

        # Cache the data
        _data_cache = {
            'df': df,
            'grouped_multiples': grouped_multiples,
            'grouped_multiple_ohne_Kanton': grouped_multiple_ohne_Kanton,
            'start_date': start_date,
            'end_date': end_date
        }

        _generation_status['state'] = 'complete'
        _generation_status['message'] = 'Data generation complete!'
        logger.info("Data generation complete! Application ready.")

    except Exception as e:
        logger.error(f"Error generating data: {str(e)}")
        _generation_status['state'] = 'error'
        _generation_status['message'] = f"Error: {str(e)}"

@app.route("/")
def home():
    global _data_cache, _generation_status
    
    # If data is ready, show it
    if _data_cache is not None:
        logger.info("Rendering visualisation.html")
        return render_template('visualisation.html')
    
    # If not ready, check if processing
    if _generation_status['state'] == 'processing':
        return render_template('loading.html')

    # If completed but cache is None (should not happen normally unless restarted and cache lost but status remained? No variables reset on restart)
    # Actually if we restart the server, variables reset.
    # So if state is idle, or complete (but cache lost), or error, restart.
    
    if _generation_status['state'] == 'idle' or _generation_status['state'] == 'error' or (_generation_status['state'] == 'complete' and _data_cache is None):
        # Start background thread
        thread = threading.Thread(target=generate_data_task)
        thread.daemon = True
        thread.start()
        return render_template('loading.html')

    return render_template('loading.html')

@app.route("/progress")
def progress():
    return jsonify(_generation_status)

@app.route("/visualize")
def visualize():
    return None
