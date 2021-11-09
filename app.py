# %%
import xml.etree.ElementTree as ET
import pandas as pd
from pandas.core.indexes.base import ensure_index_from_sequences
from pandas.tseries.offsets import BDay
pd.options.mode.chained_assignment = None  # default='warn'
from datetime import date, timedelta, datetime
import requests
import seaborn as sns
import matplotlib.pyplot as plt
import os
import pickle

def daterange(start_date, end_date):
    dates=[]
    while start_date <= end_date:
        dates.append(start_date)
        start_date += timedelta(days=1)
    return dates

def element_text(element):
    if element is None:
        return '--'
    else:
        return element.text

pickles_folder = './shab_data'

def Get_Shab_DF(download_date):
    download_date_str = download_date.strftime("%Y-%m-%d")
    pickle_file = pickles_folder +'/shab-' + download_date_str +'.pkl'
    if os.path.isfile(pickle_file):
        return pd.read_pickle(pickle_file)
    else:
        data=[]
        pages=[0,1]
        for page in pages: 
            xmlfile= './import/shab_'+download_date_str+'_'+str(page+1)+'.xml'

            url = 'https://amtsblattportal.ch/api/v1/publications/xml?publicationStates=PUBLISHED&tenant=shab&rubrics=HR&rubrics=KK&rubrics=LS&rubrics=NA&rubrics=SR&publicationDate.start='+download_date_str+'&publicationDate.end='+download_date_str+'&pageRequest.size=3000&pageRequest.sortOrders&pageRequest.page=' + str(page)
            r = requests.get(url, allow_redirects=True)
            open(xmlfile, 'wb').write(r.content)

            tree = ET.parse(xmlfile)
            root = tree.getroot()

            try:
                for rls in root.findall('./publication/meta'):

                    inner = {}
                    inner['id'] = element_text(rls.find('id'))
                    inner['date'] = element_text(rls.find('publicationDate'))
                    inner['title'] = element_text(rls.find('title/de'))   
                    inner['rubric'] = element_text(rls.find('rubric'))
                    inner['subrubric'] = element_text(rls.find('subRubric'))   
                    inner['publikations_status'] = element_text(rls.find('publicationState'))
                    inner['primaryTenantCode'] = element_text(rls.find('primaryTenantCode'))            
                    inner['kanton'] = element_text(rls.find('cantons'))  #AttributeError: 'NoneType' object has no attribute 'text'

                    data.append(inner)
            except Exception as e:
                print('Failed process {0}: {1}', xmlfile,  str(e))
        #Nach For Pages        
        df = pd.DataFrame(data)
        df.to_pickle(pickle_file) 
        return df

def Get_Shab_DF_from_range(from_date, to_date):
    df_Result = None
    for date in daterange(from_date, to_date):
        df = Get_Shab_DF(date)
        if df_Result is None:
            df_Result = df
        else:
            df_Result = pd.concat([df_Result, df],ignore_index=True)            
    return df_Result

df=Get_Shab_DF_from_range(date(2021,1,1), date(2021,10,31))

df

# %%
print('1 '+str(len(df)))
df.drop_duplicates(inplace=True)
print('2 '+str(len(df)))
# %%

HR03 = df[df["subrubric"] == 'HR03']
HR03 = HR03[['date','subrubric','kanton']]
HR03['date'] = pd.to_datetime(df['date'])
HR03_Count = HR03.groupby(pd.Grouper(key='date', freq='1M')).count()
HR03_Count.rename({'subrubric': 'HR03'}, axis=1, inplace=True)
HR01 = df[df["subrubric"] == 'HR01']
HR01 = HR01[['date','subrubric','kanton']]
HR01['date'] = pd.to_datetime(df['date'])
HR01_Count = HR01.groupby(pd.Grouper(key='date', freq='1M')).count()
HR01_Count.rename({'subrubric': 'HR01'}, axis=1, inplace=True)

HR01_03 = pd.merge(HR01_Count, HR03_Count, on="date")

fig_dims = (20,6)
fig, ax = plt.subplots(figsize=fig_dims)
sns.lineplot(data=HR01_03, x="date", y="HR03",ax=ax)
sns.lineplot(data=HR01_03, x="date", y="HR01",ax=ax)
plt.legend(labels=["Löschungen","Neueintragungen"])
ax.plot()



