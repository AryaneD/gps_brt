import requests
from prefect import task
import pandas as pd
from prefect import task
from datetime import datetime

@task
def fetch_data():
    url = "https://dados.mobilidade.rio/gps/brt"
    response = requests.get(url)
    return response.json()

@task
def process_data(data):
    df = pd.json_normalize(data)
    filename = f"brt_data_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
    df.to_csv(filename, index=False)
    return filename