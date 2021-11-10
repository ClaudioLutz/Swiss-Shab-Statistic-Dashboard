# %%
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import date, timedelta, datetime
import requests
import seaborn as sns
import matplotlib.pyplot as plt
import os
import pickle

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
    download_date_str = download_date.strftime("%Y-%m-%d")
    pickle_file = pickles_folder + '/shab-' + download_date_str + '.pkl'
    if os.path.isfile(pickle_file):
        return pd.read_pickle(pickle_file)
    else:
        data = []
        pages = [0, 1]
        for page in pages:
            xmlfile = import_folder + '/shab_' + \
                download_date_str+'_' + str(page+1) + '.xml'

            url = 'https://amtsblattportal.ch/api/v1/publications/xml?publicationStates=PUBLISHED&tenant=shab&rubrics=HR&rubrics=KK&rubrics=LS&rubrics=NA&rubrics=SR&publicationDate.start=' + \
                download_date_str+'&publicationDate.end='+download_date_str + \
                '&pageRequest.size=3000&pageRequest.sortOrders&pageRequest.page=' + \
                str(page)
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


def Get_Shab_DF_from_range(from_date, to_date):
    df_Result = None
    for date in daterange(from_date, to_date):
        df = Get_Shab_DF(date)
        if df_Result is None:
            df_Result = df
        else:
            df_Result = pd.concat([df_Result, df], ignore_index=True)
    return df_Result


