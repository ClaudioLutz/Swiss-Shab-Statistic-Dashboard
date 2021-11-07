import xml.etree.ElementTree as ET
import pandas as pd
from pandas.tseries.offsets import BMonthEnd
pd.options.mode.chained_assignment = None  # default='warn'
from datetime import date, timedelta, datetime
import requests
import seaborn as sns
import matplotlib.pyplot as plt
import os

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


d=date.today()
offset = BMonthEnd()
end_date = offset.rollback(d)
start_date = date(2021, 3, 3)
start_date = offset.rollback(start_date)
start_date = start_date + timedelta(days=3)
data = []
destCsvFile='complete_data.csv'

for single_date in daterange(start_date, end_date):
    date = single_date.strftime("%Y-%m-%d")
    pages=[0,1]
    for page in pages: 
        xmlfile= 'data\shab_'+date+'_'+str(page+1)+'.xml'
        if os.path.isfile(xmlfile) != True:
            url = 'https://amtsblattportal.ch/api/v1/publications/xml?publicationStates=PUBLISHED&tenant=shab&rubrics=HR&rubrics=KK&rubrics=LS&rubrics=NA&rubrics=SR&publicationDate.start='+date+'&publicationDate.end='+date+'&pageRequest.size=3000&pageRequest.sortOrders&pageRequest.page=' + str(page)
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
    
        df = pd.DataFrame(data)

#df = df.iloc[: , 1:]
df = df[(df["subrubric"] == "HR01") | (df["subrubric"] == "HR03")]
print(df.count())
