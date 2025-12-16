# %%
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import date, timedelta, datetime
import requests
import seaborn as sns
import matplotlib.pyplot as plt
import os
import pickle
import logging

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
    pickle_file = pickles_folder + '/shab-' + download_date_str + '.pkl'
    if os.path.isfile(pickle_file):
        logger.debug(f"Using cached data for {download_date_str}")
        return pd.read_pickle(pickle_file)
    else:
        logger.info(f"Downloading data for {download_date_str}...")
        data = []
        pages = [0, 1]
        for page in pages:
            xmlfile = import_folder + '/shab_' + \
                download_date_str+'_' + str(page+1) + '.xml'

            url = 'https://amtsblattportal.ch/api/v1/publications/xml?publicationStates=PUBLISHED&tenant=shab&rubrics=HR&rubrics=KK&rubrics=LS&rubrics=NA&rubrics=SR&publicationDate.start=' + \
                download_date_str+'&publicationDate.end='+download_date_str + \
                '&pageRequest.size=3000&pageRequest.sortOrders&pageRequest.page=' + \
                str(page)
            logger.debug(f"Fetching page {page+1} for {download_date_str}")
            r = requests.get(url, allow_redirects=True)
            open(xmlfile, 'wb').write(r.content)

            tree = ET.parse(xmlfile)
            root = tree.getroot()
            os.remove(import_folder + '/shab_' +
                      download_date_str+'_' + str(page+1) + '.xml')

            try:
                for rls in root.findall('./publication/meta'):

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
            except Exception as e:
                print('Failed process {0}: {1}', xmlfile,  str(e))
        # Nach For Pages
        df = pd.DataFrame(data)
        if df.empty == False:
            df = df[(df["subrubric"] == "HR01") | (df["subrubric"] == "HR03")]
        df.to_pickle(pickle_file)
        return df

def Get_Shab_DF_from_range(from_date, to_date, progress_callback=None):
    df_Result = None
    main_pickle = './shab_data/last_df.pkl'
    if os.path.exists(main_pickle):
        logger.info("Found cached dataset, checking date range...")
        try:
            df_Result = pd.read_pickle(main_pickle)
        except Exception as e:
            logger.warning(f"Error reading cache file {main_pickle}: {e}")
            logger.warning("Deleting corrupted cache file and restarting download.")
            os.remove(main_pickle)
            return Get_Shab_DF_from_range(from_date, to_date, progress_callback)

        df_Result['date'] = pd.to_datetime(df_Result['date']).dt.date
        # from_date and to_date are in range
        if df_Result.date.min() <= (from_date + timedelta(days=3)) and df_Result.date.max() >= (to_date - timedelta(days=3)):
            logger.info(f"Using cached data (covers {df_Result.date.min()} to {df_Result.date.max()})")
            df_Result = df_Result[(df_Result["date"] <= to_date) & (
                df_Result["date"] >= from_date)]
            return df_Result
        # from_date is out of range
        if df_Result.date.min() > (from_date + timedelta(days=3)):
            dates_to_fetch = daterange(from_date, df_Result.date.min())
            logger.info(f"Need to fetch {len(dates_to_fetch)} days of historical data...")
            for i, date in enumerate(dates_to_fetch):
                if progress_callback:
                    progress_callback(i + 1, len(dates_to_fetch), f"Fetching historical data for {date}")
                if i % 10 == 0:
                    logger.info(f"Progress: {i}/{len(dates_to_fetch)} days fetched")
                df = Get_Shab_DF(date)
                df_Result = pd.concat([df_Result, df], ignore_index=True)
            df_Result['date'] = pd.to_datetime(df_Result['date']).dt.date
            df_Result.to_pickle(main_pickle)
            logger.info("Historical data fetch complete")
            return df_Result
        # to_date is out of range
        if df_Result.date.max() < (to_date - timedelta(days=3)):
            dates_to_fetch = daterange(df_Result.date.max(), to_date)
            logger.info(f"Need to fetch {len(dates_to_fetch)} days of recent data...")
            for i, date in enumerate(dates_to_fetch):
                if progress_callback:
                    progress_callback(i + 1, len(dates_to_fetch), f"Fetching recent data for {date}")
                if i % 10 == 0:
                    logger.info(f"Progress: {i}/{len(dates_to_fetch)} days fetched")
                df = Get_Shab_DF(date)
                df_Result = pd.concat([df_Result, df], ignore_index=True)
            df_Result['date'] = pd.to_datetime(df_Result['date']).dt.date
            df_Result.to_pickle(main_pickle)
            logger.info("Recent data fetch complete")
            return df_Result
    else:
        dates_to_fetch = daterange(from_date, to_date)
        logger.info(f"No cached data found. Fetching {len(dates_to_fetch)} days of data from scratch...")
        for i, date in enumerate(dates_to_fetch):
            if progress_callback:
                progress_callback(i + 1, len(dates_to_fetch), f"Fetching initial data for {date}")
            if i % 10 == 0:
                logger.info(f"Progress: {i}/{len(dates_to_fetch)} days fetched")
            df = Get_Shab_DF(date)
            if df_Result is None:
                df_Result = df
            else:
                df_Result = pd.concat([df_Result, df], ignore_index=True)
        df_Result.to_pickle(main_pickle)
        logger.info(f"Initial data fetch complete! Total records: {len(df_Result)}")
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
