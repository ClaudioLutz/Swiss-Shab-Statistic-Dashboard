
import os
import sys
import logging
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
import json

# Import components
from app import Get_Shab_DF_from_range
from bfs_pxweb import fetch_udemo, CANTON_ABBR_TO_LABEL
from plots import generate_plots
from parquet_utils import acquire_lock, safe_write_parquet_atomic
from logging_setup import configure_logging
from dashboard_data import export_dashboard_data

from logging_setup import configure_logging

logger = logging.getLogger("refresh_data")

SHAB_DATA_DIR = './shab_data'
LOCK_FILE = os.path.join(SHAB_DATA_DIR, 'refresh.lock')
UDEMO_MERGED_FILE = os.path.join(SHAB_DATA_DIR, 'udemo_merged.parquet')
STATUS_FILE = './static/status.json'

def main():
    logger.info("Starting data refresh process...")

    # Ensure directories
    if not os.path.exists(SHAB_DATA_DIR):
        os.makedirs(SHAB_DATA_DIR)

    try:
        with acquire_lock(LOCK_FILE, timeout=10):
            # 1. Calculate Date Range (last 3 years)
            given_date = datetime.today().date()
            end_date = given_date.replace(day=1) - timedelta(days=1)
            start_date = end_date - relativedelta(years=3) + timedelta(days=1)

            logger.info(f"Target date range: {start_date} to {end_date}")

            # 2. Fetch SHAB Data
            def progress_callback(current, total, message):
                if current % 10 == 0 or current == total:
                    logger.info(f"SHAB Progress {current}/{total}: {message}")

            df_shab = Get_Shab_DF_from_range(start_date, end_date, progress_callback=progress_callback)
            logger.info(f"SHAB data fetched: {len(df_shab)} records")

            if df_shab.empty:
                logger.warning("No SHAB data found. Plots will be empty.")

            # 3. Generate Plots
            generate_plots(df_shab, start_date, end_date)

            # 4. Fetch BFS Data & Merge
            logger.info("Fetching BFS UDEMO data...")

            # Prepare SHAB data for merge
            if not df_shab.empty:
                df_shab_proc = df_shab.copy()
                df_shab_proc["year"] = pd.to_datetime(df_shab_proc["date"]).dt.year

                shab_year_canton = (
                    df_shab_proc.groupby(["kanton", "year"])
                           .size()
                           .reset_index(name="shab_events")
                )

                years = sorted(shab_year_canton["year"].unique().tolist())

                # Fetch BFS data
                try:
                    df_bfs = fetch_udemo(
                        observation_text="Unternehmensneugründungen",
                        years=years,
                        canton_abbrs=None
                    )
                except Exception as e:
                    logger.error(f"BFS fetch failed, proceeding without BFS data: {e}")
                    df_bfs = pd.DataFrame()

                if not df_bfs.empty:
                     # df_bfs columns: Beobachtungseinheit, Kanton, Rechtsform, Jahr, value
                     # Sum over legal forms
                    df_bfs_agg = df_bfs.groupby(["Jahr", "Kanton"], as_index=False)["value"].sum()
                    df_bfs_agg = df_bfs_agg.rename(columns={"Jahr": "year", "Kanton": "kanton_name", "value": "bfs_births"})

                    # Convert BFS canton names back to abbreviations
                    name_to_abbr = {v: k for k, v in CANTON_ABBR_TO_LABEL.items()}
                    df_bfs_agg["kanton"] = df_bfs_agg["kanton_name"].map(name_to_abbr)

                    # Ensure types match
                    df_bfs_agg["year"] = pd.to_numeric(df_bfs_agg["year"], errors='coerce')
                    shab_year_canton["year"] = shab_year_canton["year"].astype(int)

                    udemo_merged = shab_year_canton.merge(
                        df_bfs_agg[["kanton", "year", "bfs_births"]],
                        on=["kanton", "year"],
                        how="left"
                    )

                    logger.info(f"Merged BFS data: {len(udemo_merged)} rows.")

                    # Save merged data
                    safe_write_parquet_atomic(udemo_merged, UDEMO_MERGED_FILE)
                else:
                    logger.warning("BFS data empty, skipping merge.")

            # 5. Export Dashboard Data
            export_dashboard_data(df_shab)

            # 6. Write Status
            now = datetime.now()
            status = {
                "last_refresh": now.isoformat(),
                "data_updated_at": now.isoformat(),
                "start_date": str(start_date),
                "end_date": str(end_date),
                "records": len(df_shab),
                "status": "success",
                "data_files": ["shab_monthly.json", "dimensions.json"],
                # Basic metadata derived from df_shab if needed, or rely on dimensions.json
                "data_version": int(now.timestamp())
            }
            with open(STATUS_FILE, 'w') as f:
                json.dump(status, f)

            logger.info("Refresh completed successfully.")

    except TimeoutError:
        logger.error("Could not acquire lock. Another refresh process might be running.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Refresh failed: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    configure_logging(level="INFO", log_file="refresh.log")
    main()
