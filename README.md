# SHAB Statistics Visualizer

A Flask-based application that retrieves, analyzes, and visualizes data from the [Swiss Official Gazette of Commerce (SHAB / SOGC)](https://amtsblattportal.ch). This tool tracks commercial registry publications—specifically new entries (HR01) and deletions (HR03)—over the last three years, providing insights into business trends across different Swiss cantons.

## Features

- **Automated Data Retrieval**: Downloads daily publication data (XML) directly from the SHAB API.
- **Efficient Data Caching**: Parsed data is stored in local pickle files (`.pkl`) to minimize network requests and accelerate subsequent runs.
- **Interactive Visualizations**:
  - **Trend Analysis**: A line graph displaying the volume of new entries vs. deletions over time.
  - **Geographic Breakdown**: A facet grid showing publication trends broken down by canton.
- **Web Dashboard**: A simple web interface to view the generated visualizations.

## Prerequisites

- **Python**: Version 3.9
- **Pipenv**: For dependency management. [Install Pipenv](https://pipenv.pypa.io/en/latest/installation/) if you haven't already.

## Installation

1.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd <repository-directory>
    ```

2.  **Install dependencies:**
    ```bash
    pipenv install
    ```

## Usage

1.  **Start the Application:**

    Use Flask to run the application.

    > **Note:** Upon initialization, the application calculates a date range for the last 3 years and attempts to download and process the corresponding SHAB data. **The first run may take a significant amount of time** (potentially several minutes or more) depending on your internet connection, as it fetches daily records. Subsequent runs will use the cached data in the `shab_data/` directory.

    ```bash
    pipenv run flask --app flask_seaborn run
    ```

2.  **View the Dashboard:**

    Once the server is running (and data processing is complete), open your web browser and navigate to:

    [http://127.0.0.1:5000/](http://127.0.0.1:5000/)

    You will see the generated visualizations for the requested period.

## Project Structure

- **`flask_seaborn.py`**: The entry point for the Flask application. It triggers the data loading/processing pipeline and serves the web page.
- **`app.py`**: Contains the core logic for:
    - Downloading XML data from the SHAB API.
    - Parsing and filtering data (HR01/HR03).
    - Managing the local cache (pickle files).
    - Generating Seaborn/Matplotlib plots.
- **`templates/visualisation.html`**: The HTML template for the dashboard.
- **`static/`**: Directory where generated plots (`LineGraph.png`, `FacetGridKanton.png`) are saved and served from.
- **`shab_data/`**: Local cache directory storing processed DataFrames.
- **`import/`**: Temporary directory used during the download of XML files.

## Data Source

This application uses data provided by the **Swiss Official Gazette of Commerce (SHAB)** via their [Amtsblattportal](https://amtsblattportal.ch). It specifically filters for:
- **HR01**: Neueintragungen (New Entries)
- **HR03**: Löschungen (Deletions)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
