# SHAB Statistics Visualizer

A Flask-based application that retrieves, analyzes, and visualizes data from the [Swiss Official Gazette of Commerce (SHAB / SOGC)](https://amtsblattportal.ch). This tool tracks commercial registry publications—specifically new entries (HR01) and deletions (HR03)—over the last three years, providing insights into business trends across different Swiss cantons.

The primary workflow is:

1. **Refresh artifacts** (downloads + caches + generates plots)
2. **Run Flask** (serves already-generated artifacts)

## Quickstart

### Prerequisites
- Python 3.11 (recommended)
- pipenv

### Install
```bash
pipenv --python 3.11
pipenv install
```

### Refresh data and generate artifacts
```bash
pipenv run python refresh_data.py
```

### Run the dashboard
```bash
pipenv run python flask_seaborn.py
# or:
# pipenv run flask --app flask_seaborn run
```

## Generated artifacts

The refresh step writes:
- SHAB daily cache: `shab_data/shab-YYYY-MM-DD.parquet`
- Aggregated cache: `shab_data/last_df.parquet`
- Optional merged UDEMO dataset: (e.g.) `shab_data/udemo_merged.parquet`
- Plots:
  - `static/LineGraph.png`
  - `static/FacetGridKanton.png`
- Refresh metadata:
  - `static/status.json`

The Flask app serves these artifacts and does not download/process SHAB data during HTTP requests.

## Features

- **Automated Data Retrieval**: Downloads daily publication data (XML) directly from the SHAB API.
- **Efficient Data Caching**: Parsed data is stored in local parquet files to minimize network requests and accelerate subsequent runs.
- **Interactive Visualizations**:
  - **Trend Analysis**: A line graph displaying the volume of new entries vs. deletions over time.
  - **Geographic Breakdown**: A facet grid showing publication trends broken down by canton.
- **Web Dashboard**: A simple web interface to view the generated visualizations.

## Project Structure

- **`refresh_data.py`**: The CLI entry point for data orchestrator (download, process, plot).
- **`flask_seaborn.py`**: The entry point for the Flask application. Serves the web page.
- **`app.py`**: Contains the core logic for downloading and parsing SHAB data.
- **`templates/visualisation.html`**: The HTML template for the dashboard.
- **`static/`**: Directory where generated plots (`LineGraph.png`, `FacetGridKanton.png`) are saved and served from.
- **`shab_data/`**: Local cache directory storing processed DataFrames (Parquet).
- **`parquet_utils.py`**: Utilities for safe Parquet operations and file locking.
- **`bfs_pxweb.py`**: Module for interacting with the BFS PxWeb API.

## Data Source

This application uses data provided by the **Swiss Official Gazette of Commerce (SHAB)** via their [Amtsblattportal](https://amtsblattportal.ch). It specifically filters for:
- **HR01**: Neueintragungen (New Entries)
- **HR03**: Löschungen (Deletions)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
