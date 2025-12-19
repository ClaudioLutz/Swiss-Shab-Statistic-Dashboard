# %%
import os
# Fix for PyArrow/Pandas compatibility issue - must be set before importing pandas
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"

import xml.etree.ElementTree as ET
import pandas as pd
from datetime import date, timedelta, datetime
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging
import io
import time

# Import safe parquet utilities
from parquet_utils import safe_read_parquet, safe_write_parquet_atomic, acquire_lock

# Configure logging
logger = logging.getLogger(__name__)

# Constants
SHAB_DATA_DIR = './shab_data'
IMPORT_FOLDER = './import'
STATIC_FOLDER = './static'
LOCK_FILE = os.path.join(SHAB_DATA_DIR, 'refresh.lock')

def ensure_directories():
    for folder in [SHAB_DATA_DIR, IMPORT_FOLDER, STATIC_FOLDER]:
        if not os.path.exists(folder):
            os.makedirs(folder)

def daterange(start_date, end_date):
    dates = []
    curr = start_date
    while curr <= end_date:
        dates.append(curr)
        curr += timedelta(days=1)
    return dates

def element_text(element):
    if element is None:
        return '--'
    else:
        return element.text

def get_session():
    session = requests.Session()
    # Retry on:
    # 429: Too Many Requests
    # 500: Internal Server Error
    # 502: Bad Gateway
    # 503: Service Unavailable
    # 504: Gateway Timeout
    retry = Retry(
        total=5,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def Get_Shab_DF(download_date, session=None):
    ensure_directories()
    download_date_str = download_date.strftime("%Y-%m-%d")
    parquet_file = os.path.join(SHAB_DATA_DIR, f'shab-{download_date_str}.parquet')

    if os.path.isfile(parquet_file):
        logger.debug(f"Using cached data for {download_date_str}")
        return safe_read_parquet(parquet_file)

    logger.info(f"Downloading data for {download_date_str}...")

    # Define columns for empty dataframe
    columns = ['id', 'date', 'title', 'rubric', 'subrubric', 'publikations_status', 'primaryTenantCode', 'kanton']
    data = []
    page = 0

    if session is None:
        session = get_session()

    while True:
        # Reduced rubrics to HR only as we filter for HR01 and HR03 later
        url = (
            'https://amtsblattportal.ch/api/v1/publications/xml'
            '?publicationStates=PUBLISHED&tenant=shab&rubrics=HR'
            f'&publicationDate.start={download_date_str}'
            f'&publicationDate.end={download_date_str}'
            '&pageRequest.size=3000&pageRequest.sortOrders'
            f'&pageRequest.page={page}'
        )

        logger.debug(f"Fetching page {page+1} for {download_date_str}")
        try:
            r = session.get(url, allow_redirects=True, timeout=(10, 30))
            r.raise_for_status()

            # Parse from memory
            try:
                tree = ET.parse(io.BytesIO(r.content))
                root = tree.getroot()
            except ET.ParseError:
                logger.error(f"Failed to parse XML for {download_date_str} page {page}")
                break # Or raise, depending on desired robustness. Here we break to save what we have or empty.

            publications = root.findall('./publication/meta')
            if not publications:
                break # No more publications found

            for rls in publications:
                inner = {}
                inner['id'] = element_text(rls.find('id'))
                inner['date'] = element_text(rls.find('publicationDate'))
                inner['title'] = element_text(rls.find('title/de'))
                inner['rubric'] = element_text(rls.find('rubric'))
                inner['subrubric'] = element_text(rls.find('subRubric'))
                inner['publikations_status'] = element_text(rls.find('publicationState'))
                inner['primaryTenantCode'] = element_text(rls.find('primaryTenantCode'))
                inner['kanton'] = element_text(rls.find('cantons'))

                data.append(inner)

            page += 1

            # Simple safety breaker for infinite loops
            if page > 100:
                logger.warning(f"Exceeded 100 pages for {download_date_str}, stopping.")
                break

        except Exception as e:
            logger.error(f"Failed to fetch or process page {page} for {download_date_str}: {str(e)}")
            # If we fail, we probably shouldn't save an incomplete file unless we want to cache failure.
            # But the requirement is to not fail the whole process.
            # Let's raise here so retry logic can handle it or the caller handles it.
            # For now, we will stop and try to save what we have or empty if it was a persistent error.
            # But to be safe, let's propagate network errors.
            raise e

    df = pd.DataFrame(data, columns=columns)

    if not df.empty:
        df = df[(df["subrubric"] == "HR01") | (df["subrubric"] == "HR03")]
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
    else:
        # Ensure schema even if empty
        if 'date' in df.columns:
             df['date'] = pd.to_datetime(df['date'])

    # Save as parquet (even if empty, to mark as processed)
    safe_write_parquet_atomic(df, parquet_file)
    return df

def Get_Shab_DF_from_range(from_date, to_date, progress_callback=None):
    ensure_directories()
    main_parquet = os.path.join(SHAB_DATA_DIR, 'last_df.parquet')

    # We will use a session for reuse
    session = get_session()

    # We need to accumulate data.
    # Strategy:
    # 1. Load main_parquet if exists.
    # 2. Determine missing ranges.
    # 3. Fetch missing ranges.
    # 4. Update main_parquet.

    df_cached = pd.DataFrame()
    cached_start = None
    cached_end = None

    if os.path.exists(main_parquet):
        logger.info("Found aggregated dataset.")
        df_cached = safe_read_parquet(main_parquet)
        if df_cached is not None and not df_cached.empty:
            df_cached['date'] = pd.to_datetime(df_cached['date'])
            cached_start = df_cached.date.min().date()
            cached_end = df_cached.date.max().date()
            logger.info(f"Cached data covers {cached_start} to {cached_end}")

    # Calculate days to fetch
    days_to_fetch = []

    # If no cache, fetch all
    if cached_start is None:
        days_to_fetch = daterange(from_date, to_date)
    else:
        # Check gap before cache
        if from_date < cached_start:
            days_to_fetch.extend(daterange(from_date, cached_start - timedelta(days=1)))

        # Check gap after cache
        if to_date > cached_end:
            days_to_fetch.extend(daterange(cached_end + timedelta(days=1), to_date))

    # Remove days that might be in the middle gap if any (though we assume continuous cache)
    # But also check if individual daily files exist to avoid re-downloading even if not in aggregated file

    final_days_to_fetch = []
    for day in days_to_fetch:
        day_str = day.strftime("%Y-%m-%d")
        daily_file = os.path.join(SHAB_DATA_DIR, f'shab-{day_str}.parquet')
        if not os.path.exists(daily_file):
            final_days_to_fetch.append(day)

    days_to_fetch = final_days_to_fetch
    days_to_fetch.sort()

    logger.info(f"Need to fetch {len(days_to_fetch)} days...")

    # Fetch missing days
    new_data_frames = []
    total_days = len(days_to_fetch)

    for i, date_curr in enumerate(days_to_fetch):
        if progress_callback:
            progress_callback(i + 1, total_days, f"Fetching data for {date_curr}")
        if i % 10 == 0:
            logger.info(f"Progress: {i}/{total_days} days fetched")

        try:
            df = Get_Shab_DF(date_curr, session=session)
            if not df.empty:
                # Ensure date is datetime64
                df['date'] = pd.to_datetime(df['date'])
                new_data_frames.append(df)
        except Exception as e:
            logger.error(f"Error fetching {date_curr}: {e}")
            # Continue to next day? Yes.

    # Concatenate everything
    # 1. Start with cached data
    dfs_to_concat = [df_cached] if not df_cached.empty else []

    # 2. Add newly fetched data (in memory)
    if new_data_frames:
        dfs_to_concat.extend(new_data_frames)

    # 3. Also look for daily files that were already on disk but not in cached_df (gap filling logic simplified)
    # Actually, simpler approach: Reload ALL daily files within the requested range to ensure consistency
    # This might be slow if there are many files.
    # Optimization: Just append new frames to cached and dedup.

    if dfs_to_concat:
        df_result = pd.concat(dfs_to_concat, ignore_index=True)
    else:
        df_result = pd.DataFrame()

    if not df_result.empty:
        if 'id' in df_result.columns:
            df_result = df_result.drop_duplicates(subset=['id'])

        df_result['date'] = pd.to_datetime(df_result['date'])

        # Save updated aggregated file
        safe_write_parquet_atomic(df_result, main_parquet)

        # Filter for return
        df_result = df_result[(df_result["date"].dt.date <= to_date) & (df_result["date"].dt.date >= from_date)]

    return df_result
