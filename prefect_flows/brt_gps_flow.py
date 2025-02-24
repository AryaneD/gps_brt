from prefect import task, Flow
import requests
import pandas as pd
import re
from datetime import datetime
from sqlalchemy import create_engine

# Tarefa para buscar os dados da API
@task
def fetch_data():
    url = "https://dados.mobilidade.rio/gps/brt"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        raise ValueError(f"Erro ao obter dados da API: {e}")

# Tarefa para processar os dados
@task
def process_data(data):
    # Extraindo a lista de veículos corretamente
    df = pd.json_normalize(data, "veiculos")

    # Convertendo a coluna 'dataHora' de milissegundos para datetime
    df["dataHora"] = pd.to_datetime(df["dataHora"], unit="ms")

    # Gerando o nome do arquivo CSV
    filename = f"brt_data_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
    df.to_csv(filename, index=False)

    return filename

# Tarefa para carregar os dados no PostgreSQL
@task
def load_to_postgresql(csv_file):
    try:
        engine = create_engine("postgresql://user:password@127.0.0.1:5433/brt_db?client_encoding=utf8")
        # Lendo o CSV e carregando para o PostgreSQL
        df = pd.read_csv(csv_file, encoding='utf-8')
        df.to_sql("brt_gps_data", engine, if_exists="append", index=False)

    except Exception as e:
        raise ValueError(f"Erro ao carregar dados no PostgreSQL: {e}")

# Criando o fluxo no Prefect
with Flow("brt_gps_flow") as flow:
    data = fetch_data()
    filename = process_data(data)
    load_to_postgresql(filename)

# Rodando o fluxo
if __name__ == "__main__":
    flow.run()

