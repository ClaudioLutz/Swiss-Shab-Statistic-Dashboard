from flask import Flask, send_file, render_template
import io
import base64
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
import matplotlib.pyplot as plt
import seaborn as sns
from app import Get_Shab_DF_from_range, grouped_multiple, FacetGridKanton, grouped_multiple_ohne_Kantone, LineGraph
from datetime import date, time, timedelta, datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Global variable to cache the data
_data_cache = None

def get_or_generate_data():
    """Generate data and plots if they don't exist yet"""
    global _data_cache
    
    # Check if data is already cached
    if _data_cache is not None:
        logger.info("Using cached data")
        return _data_cache
    
    logger.info("Starting data generation process...")
    
    # Calculate date range
    given_date = datetime.today().date() 
    end_date = given_date.replace(day=1) - timedelta(days=1)
    start_date = end_date - relativedelta(years=3)
    start_date = start_date + timedelta(days=1)
    
    logger.info(f"Fetching data from {start_date} to {end_date}")
    
    # Get data
    df = Get_Shab_DF_from_range(start_date, end_date)
    logger.info(f"Successfully fetched {len(df)} records")
    
    # Process data
    logger.info("Processing data - grouping by month, subrubric, and kanton...")
    grouped_multiples = grouped_multiple(df)
    
    # Generate plots
    logger.info("Generating FacetGrid plot for Kantons...")
    FacetGridKanton(grouped_multiples, start_date, end_date)
    logger.info("FacetGrid plot saved to ./static/FacetGridKanton.png")
    
    logger.info("Processing data without Kantons...")
    grouped_multiple_ohne_Kanton = grouped_multiple_ohne_Kantone(df)
    
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
    
    logger.info("Data generation complete! Application ready.")
    return _data_cache

@app.route("/")
def home():
    # Generate data on first request if needed
    logger.info("Home route accessed - checking/generating data...")
    get_or_generate_data()
    logger.info("Rendering visualisation.html")
    return render_template('visualisation.html')

@app.route("/visualize")
def visualize():
    return None
