from prefect import task, Flow
import requests
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine

# Defina a tarefa para capturar os dados
@task
def fetch_data():
    url = "https://dados.mobilidade.rio/gps/brt"
    response = requests.get(url)
    return response.json()

# Defina a tarefa para processar os dados e salvar em CSV
@task
def process_data(data):
    df = pd.json_normalize(data)
    filename = f"brt_data_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
    df.to_csv(filename, index=False)
    return filename

# Defina a tarefa para carregar os dados no PostgreSQL
@task
def load_to_postgresql(csv_file):
    df = pd.read_csv(csv_file)
    engine = create_engine('postgresql://user:password@localhost:5432/brt_db')
    df.to_sql('brt_gps_data', con=engine, if_exists='append', index=False)

# Crie o flow
with Flow("brt_gps_flow") as flow:
    data = fetch_data()
    filename = process_data(data)
    load_to_postgresql(filename)

# Rodando o flow
if __name__ == "__main__":
    flow.run()

#Tarefa 1: Captura os dados da API.
#Tarefa 2: Processa e gera o CSV.
#Tarefa 3: Carrega os dados no banco PostgreSQL.