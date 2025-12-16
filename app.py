# %%
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import date, timedelta, datetime
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import seaborn as sns
import matplotlib.pyplot as plt
import os
import pickle
import logging
import io

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def daterange(start_date, end_date):
    dates = []
    while start_date <= end_date:
        dates.append(start_date)
        start_date += timedelta(days=1)
    return dates

def element_text(element):
    if element is None:
        return '--'
    else:
        return element.text

def get_session():
    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def Get_Shab_DF(download_date):
    pickles_folder = './shab_data'
    if os.path.exists(pickles_folder) == False:
        os.mkdir(pickles_folder)
    import_folder = './import'
    if os.path.exists(import_folder) == False:
        os.mkdir(import_folder)
    static_folder = './static'
    if os.path.exists(static_folder) == False:
        os.mkdir(static_folder)

    download_date_str = download_date.strftime("%Y-%m-%d")
    # Change extension to .parquet
    parquet_file = pickles_folder + '/shab-' + download_date_str + '.parquet'

    if os.path.isfile(parquet_file):
        logger.debug(f"Using cached data for {download_date_str}")
        return pd.read_parquet(parquet_file)
    else:
        logger.info(f"Downloading data for {download_date_str}...")
        data = []
        page = 0
        session = get_session()

        while True:
            # Reduced rubrics to HR only as we filter for HR01 and HR03 later
            url = 'https://amtsblattportal.ch/api/v1/publications/xml?publicationStates=PUBLISHED&tenant=shab&rubrics=HR&publicationDate.start=' + \
                download_date_str+'&publicationDate.end='+download_date_str + \
                '&pageRequest.size=3000&pageRequest.sortOrders&pageRequest.page=' + \
                str(page)

            logger.debug(f"Fetching page {page+1} for {download_date_str}")
            try:
                r = session.get(url, allow_redirects=True, timeout=(10, 30))
                r.raise_for_status()

                # Parse from memory
                tree = ET.parse(io.BytesIO(r.content))
                root = tree.getroot()

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
                    inner['publikations_status'] = element_text(
                        rls.find('publicationState'))
                    inner['primaryTenantCode'] = element_text(
                        rls.find('primaryTenantCode'))
                    inner['kanton'] = element_text(rls.find('cantons'))

                    data.append(inner)

                page += 1

            except Exception as e:
                logger.error(f"Failed to fetch or process page {page} for {download_date_str}: {str(e)}")
                raise e

        # Nach For Pages
        df = pd.DataFrame(data)
        if df.empty == False:
            df = df[(df["subrubric"] == "HR01") | (df["subrubric"] == "HR03")]

        # Save as parquet
        # Ensure date column is compatible with parquet
        if not df.empty and 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])

        df.to_parquet(parquet_file)
        return df

def Get_Shab_DF_from_range(from_date, to_date, progress_callback=None):
    df_Result = None
    main_parquet = './shab_data/last_df.parquet'

    if os.path.exists(main_parquet):
        logger.info("Found cached dataset, checking date range...")
        df_Result = pd.read_parquet(main_parquet)
        # Ensure date is datetime64 for processing, then convert to date for comparisons
        df_Result['date'] = pd.to_datetime(df_Result['date'])

        # Calculate min/max dates
        dataset_start = df_Result.date.min().date()
        dataset_end = df_Result.date.max().date()

        if dataset_start <= from_date and dataset_end >= to_date:
            logger.info(f"Using cached data (covers {dataset_start} to {dataset_end})")
            # Filter using datetime64 comparison (converting input date to datetime64)
            df_Result = df_Result[(df_Result["date"] <= pd.to_datetime(to_date)) & (
                df_Result["date"] >= pd.to_datetime(from_date))]
            return df_Result

        # from_date is earlier than cached data
        if dataset_start > from_date:
            # fetch up to dataset_start - 1 day to avoid overlap
            fetch_end_date = dataset_start - timedelta(days=1)
            dates_to_fetch = daterange(from_date, fetch_end_date)

            logger.info(f"Need to fetch {len(dates_to_fetch)} days of historical data...")
            for i, date_curr in enumerate(dates_to_fetch):
                if progress_callback:
                    progress_callback(i + 1, len(dates_to_fetch), f"Fetching historical data for {date_curr}")
                if i % 10 == 0:
                    logger.info(f"Progress: {i}/{len(dates_to_fetch)} days fetched")
                df = Get_Shab_DF(date_curr)
                if not df.empty:
                     df['date'] = pd.to_datetime(df['date'])
                df_Result = pd.concat([df_Result, df], ignore_index=True)

        # to_date is later than cached data
        if dataset_end < to_date:
            # fetch from dataset_end + 1 day
            fetch_start_date = dataset_end + timedelta(days=1)
            dates_to_fetch = daterange(fetch_start_date, to_date)

            logger.info(f"Need to fetch {len(dates_to_fetch)} days of recent data...")
            for i, date_curr in enumerate(dates_to_fetch):
                if progress_callback:
                    progress_callback(i + 1, len(dates_to_fetch), f"Fetching recent data for {date_curr}")
                if i % 10 == 0:
                    logger.info(f"Progress: {i}/{len(dates_to_fetch)} days fetched")
                df = Get_Shab_DF(date_curr)
                if not df.empty:
                     df['date'] = pd.to_datetime(df['date'])
                df_Result = pd.concat([df_Result, df], ignore_index=True)

        # Deduplicate just in case
        if not df_Result.empty and 'id' in df_Result.columns:
             df_Result = df_Result.drop_duplicates(subset=['id'])

        # Keep as datetime64 before saving to Parquet
        if not df_Result.empty:
            df_Result['date'] = pd.to_datetime(df_Result['date'])
            df_Result.to_parquet(main_parquet)

        # Filter again to return only requested range
        df_Result = df_Result[(df_Result["date"] <= pd.to_datetime(to_date)) & (
                df_Result["date"] >= pd.to_datetime(from_date))]

        return df_Result
    else:
        dates_to_fetch = daterange(from_date, to_date)
        logger.info(f"No cached data found. Fetching {len(dates_to_fetch)} days of data from scratch...")
        for i, date_curr in enumerate(dates_to_fetch):
            if progress_callback:
                progress_callback(i + 1, len(dates_to_fetch), f"Fetching initial data for {date_curr}")
            if i % 10 == 0:
                logger.info(f"Progress: {i}/{len(dates_to_fetch)} days fetched")
            df = Get_Shab_DF(date_curr)
            if df_Result is None:
                df_Result = df
            else:
                if not df.empty:
                    df['date'] = pd.to_datetime(df['date'])
                df_Result = pd.concat([df_Result, df], ignore_index=True)

        if df_Result is not None and not df_Result.empty:
             if 'id' in df_Result.columns:
                df_Result = df_Result.drop_duplicates(subset=['id'])
             # Ensure date is datetime64
             df_Result['date'] = pd.to_datetime(df_Result['date'])
             df_Result.to_parquet(main_parquet)

        logger.info(f"Initial data fetch complete! Total records: {len(df_Result) if df_Result is not None else 0}")
        return df_Result

#test
#df = Get_Shab_DF_from_range(date(2020, 1, 1), date(2021, 10, 31))
#df

def FacetGridKanton(grouped_multiple, start, end):
    graphHR = sns.FacetGrid(grouped_multiple, col="kanton", col_wrap=5,
                        hue = "subrubric", sharey = False)
    start = start
    end = end
    graphHR = (graphHR.map(sns.lineplot,"month","count")
            .add_legend()
            .set_axis_labels(str(start)+" - " + str(end),"Meldungen")
            .set(xticklabels=[])
            )

    graphHR.savefig("./static/FacetGridKanton.png")

def LineGraph(grouped_multiple_ohne_Kantone):
    #fig, ax = plt.subplots(figsize=(20,6))
    sns.lineplot(data=grouped_multiple_ohne_Kantone, x="month", y='count',hue='subrubric',figsize=(20,6))
    plt.xticks(rotation=45)
    plt.savefig("./static/LineGraph.png")

def grouped_multiple(df):
    df.date = pd.to_datetime(df.date)
    df['month'] = df['date'].dt.strftime('%Y-%m')
    grouped_multiple = df.groupby(['month','subrubric','kanton']).agg({'subrubric': ['count']})
    grouped_multiple.columns = ['count']
    grouped_multiple = grouped_multiple.reset_index()
    return grouped_multiple

def grouped_multiple_ohne_Kantone(df):
    df.date = pd.to_datetime(df.date)
    df['month'] = df['date'].dt.strftime('%Y-%m')
    grouped_multiple = df.groupby(['month','subrubric']).agg({'subrubric': ['count']})
    grouped_multiple.columns = ['count']
    grouped_multiple = grouped_multiple.reset_index()
    return grouped_multiple



# %%



# %%
