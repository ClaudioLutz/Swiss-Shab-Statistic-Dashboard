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
    start_date += timedelta(days=1)
    while start_date < end_date:
        dates.append(start_date)
        start_date += timedelta(days=1)
    return dates

def element_text(element):
    if element is None:
        return '--'
    else:
        return element.text

given_date = datetime.today().date() 
end_date = given_date.replace(day=1)
start_date = date(2019, 1, 1)
df=None
data = []
picklevar = "./dummy.pkl"
xmlfolder = "./data"
if os.path.exists(xmlfolder) == False:
    os.mkdir(xmlfolder)

if os.path.isfile(picklevar) == True:
    df = pd.read_pickle(picklevar)
    df['date']= pd.to_datetime(df['date']).dt.date
    if df.date.min() <= (start_date + timedelta(days=3)) and df.date.max() >= (end_date - timedelta(days=3)):
        df = df[(df["date"] <= end_date) & (df["date"] >= start_date)]
    else:
        for single_date in daterange(start_date, end_date):
            date = single_date.strftime("%Y-%m-%d")
            pages=[0,1]
            for page in pages: 
                xmlfile= 'data\shab_'+date+'_'+str(page+1)+'.xml'
                if os.path.isfile(xmlfile) != True:
                    url = 'https://amtsblattportal.ch/api/v1/publications/xml?publicationStates=PUBLISHED&tenant=shab&rubrics=HR&rubrics=KK&rubrics=LS&rubrics=NA&rubrics=SR&publicationDate.start='+date+'&publicationDate.end='+date+'&pageRequest.size=3000&pageRequest.sortOrders&pageRequest.page=' + str(page)
                    r = requests.get(url, allow_redirects=True)
                    open(xmlfile, 'wb').write(r.content)
                    
        for single_date in daterange(start_date, end_date):
            date = single_date.strftime("%Y-%m-%d")
            pages=[0,1]            
            for page in pages: 
                xmlfile= 'data\shab_'+date+'_'+str(page+1)+'.xml'                
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
            
        if not df is None: 
            df=pd.concat([df, pd.DataFrame(data)],ignore_index=True)
        else:
            df = pd.DataFrame(data)

        df = df[(df["subrubric"] == "HR01") | (df["subrubric"] == "HR03")]
        df.to_pickle(picklevar)

if os.path.isfile(picklevar) == False:
    for single_date in daterange(start_date, end_date):
        date = single_date.strftime("%Y-%m-%d")
        pages=[0,1]
        for page in pages: 
            xmlfile= 'data\shab_'+date+'_'+str(page+1)+'.xml'
            if os.path.isfile(xmlfile) != True:
                url = 'https://amtsblattportal.ch/api/v1/publications/xml?publicationStates=PUBLISHED&tenant=shab&rubrics=HR&rubrics=KK&rubrics=LS&rubrics=NA&rubrics=SR&publicationDate.start='+date+'&publicationDate.end='+date+'&pageRequest.size=3000&pageRequest.sortOrders&pageRequest.page=' + str(page)
                r = requests.get(url, allow_redirects=True)
                open(xmlfile, 'wb').write(r.content)
                
    for single_date in daterange(start_date, end_date):
        date = single_date.strftime("%Y-%m-%d")
        pages=[0,1]            
        for page in pages: 
            xmlfile= 'data\shab_'+date+'_'+str(page+1)+'.xml'                
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
        
        df = pd.DataFrame(data)

    df = df[(df["subrubric"] == "HR01") | (df["subrubric"] == "HR03")]
    df.to_pickle(picklevar)

# %%
print('1 '+str(len(df)))
df.drop_duplicates()
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


# %%
print('1 '+str(len(df)))
df=df[(df["date"] <= end_date) & (df["date"] >= date(2021,8,1))]
print('2 '+str(len(df)))
# %%

df.to_excel("output.xlsx")

# %%

