from prefect import Flow, task
import requests
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
import os
from prefect.schedules import IntervalSchedule
import time

# Schedule: Execute a cada minuto
schedule = IntervalSchedule(interval=pd.Timedelta(minutes=1))

# Extração
@task
def fetch_data():
    try:
        url = "https://dados.mobilidade.rio/gps/brt"
        response = requests.get(url)
        response.raise_for_status()  # Garante que uma exceção seja gerada para códigos de status de erro
        return response.json()
    except requests.exceptions.RequestException as e:
        raise ValueError(f"Erro ao buscar dados: {e}")

# Processamento
@task
def process_data(data):
    try:
        df = pd.json_normalize(data)
        filename = f"brt_data_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
        df.to_csv(filename, index=False)
        return filename
    except Exception as e:
        raise ValueError(f"Erro ao processar dados: {e}")

# Carregamento incremental para o PostgreSQL
@task
def load_to_postgresql(csv_file):
    try:
        if not os.path.exists(csv_file):
            raise FileNotFoundError(f"O arquivo {csv_file} não foi encontrado.")
        df = pd.read_csv(csv_file)
        engine = create_engine('postgresql://user:password@localhost:5433/postgres')
        df.to_sql('brt_gps_data', con=engine, if_exists='append', index=False)
    except Exception as e:
        raise ValueError(f"Erro ao carregar dados para o PostgreSQL: {e}")

# Criação do fluxo do Prefect
with Flow("brt_gps_flow", schedule=schedule) as flow:
    data = fetch_data()
    filename = process_data(data)
    load_to_postgresql(filename)

# Rodando o fluxo
if __name__ == "__main__":
    flow.run()
