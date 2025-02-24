from prefect import Flow, task
import requests
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
import os
from prefect.schedules import IntervalSchedule

# Schedule: Execute a cada minuto
schedule = IntervalSchedule(interval=pd.Timedelta(minutes=1))

# Extração
@task
def fetch_data():
    try:
        url = "https://dados.mobilidade.rio/gps/brt"
        response = requests.get(url)
        response.raise_for_status()  # Garante que uma exceção seja gerada para códigos de status de erro
        return response.json()  # Retorna os dados em formato JSON
    except requests.exceptions.RequestException as e:
        raise ValueError(f"Erro ao buscar dados: {e}")

# Carregamento incremental para o PostgreSQL
@task
def load_to_postgresql(data):
    try:
        # Criar a conexão com o PostgreSQL usando SQLAlchemy
        engine = create_engine('postgresql://user:password@localhost:5433/postgres?client_encoding=utf8')

        # Preparar os dados como JSONB
        for veiculo in data["veiculos"]:
            # Aqui você pode inserir cada "veiculo" como JSON na tabela
            sql = """
                INSERT INTO brt_gps_data (veiculo) 
                VALUES (%s);
            """
            with engine.connect() as conn:
                conn.execute(sql, (str(veiculo),))  # Convertendo o JSON para string antes de inserir

    except Exception as e:
        raise ValueError(f"Erro ao carregar dados para o PostgreSQL: {e}")

# Criação do fluxo do Prefect
with Flow("brt_gps_flow", schedule=schedule) as flow:
    data = fetch_data()
    load_to_postgresql(data)

# Rodando o fluxo
if __name__ == "__main__":
    flow.run()
