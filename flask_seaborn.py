from flask import Flask,send_file,render_template
import io
import base64
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
import matplotlib.pyplot as plt
import seaborn as sns
from app import Get_Shab_DF_from_range, grouped_multiple, FacetGridKanton, grouped_multiple_ohne_Kantone, LineGraph
from datetime import date, time, timedelta, datetime
from dateutil.relativedelta import relativedelta
import pandas as pd

given_date = datetime.today().date() 
end_date = given_date.replace(day=1) - timedelta(days=1)
start_date = end_date - relativedelta(years=3)
start_date = start_date + timedelta(days=1)

df = Get_Shab_DF_from_range(start_date, end_date)

grouped_multiples = grouped_multiple(df)

FacetGridKanton(grouped_multiples,start_date,end_date)

grouped_multiple_ohne_Kanton = grouped_multiple_ohne_Kantone(df)

#LineGraph(grouped_multiple_ohne_Kantone)

fig, ax = plt.subplots(figsize=(20,6))
ax=sns.set_style(style='darkgrid')

app = Flask(__name__)

@app.route("/")
def home():
    return render_template('visualisation.html')

@app.route("/visualize")
def visualize():
    sns.lineplot(data=grouped_multiple_ohne_Kanton, x="month", y='count',hue='subrubric',ax=ax)
    plt.xticks(rotation=45)
    plt.plot()
    canvas = FigureCanvas(fig)
    img=io.BytesIO()
    fig.savefig(img)
    img.seek(0)
    return send_file(img,mimetype='img/png')
