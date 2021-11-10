from flask import Flask,send_file,render_template
import io
import base64
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
import matplotlib.pyplot as plt
import seaborn as sns
from app import Get_Shab_DF_from_range
from datetime import date, timedelta, datetime
import pandas as pd


df = Get_Shab_DF_from_range(date(2019, 1, 1), date(2021, 10, 31))

HR03 = df[df["subrubric"] == 'HR03']
HR03 = HR03[['date', 'subrubric', 'kanton']]
HR03['date'] = pd.to_datetime(df['date'])
HR03_Count = HR03.groupby(pd.Grouper(key='date', freq='1M')).count()
HR03_Count.rename({'subrubric': 'HR03'}, axis=1, inplace=True)
HR01 = df[df["subrubric"] == 'HR01']
HR01 = HR01[['date', 'subrubric', 'kanton']]
HR01['date'] = pd.to_datetime(df['date'])
HR01_Count = HR01.groupby(pd.Grouper(key='date', freq='1M')).count()
HR01_Count.rename({'subrubric': 'HR01'}, axis=1, inplace=True)

HR01_03 = pd.merge(HR01_Count, HR03_Count, on="date")


fig, ax = plt.subplots(figsize=(20,6))
ax=sns.set_style(style='darkgrid')

app = Flask(__name__)

@app.route("/")
def home():
    return render_template('visualisation.html')

@app.route("/visualize")
def visualize():
    sns.lineplot(data=HR01_03, x="date", y="HR03", ax=ax)  # 2600
    sns.lineplot(data=HR01_03, x="date", y="HR01", ax=ax)  # 3700
    plt.legend(labels=["HR03", "HR01"])
    canvas = FigureCanvas(fig)
    img=io.BytesIO()
    fig.savefig(img)
    img.seek(0)
    return send_file(img,mimetype='img/png')
