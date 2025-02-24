from prefect import Flow, task
import requests
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
<<<<<<< HEAD
import os
import time


# Extração
=======
import re

# Função para remover caracteres especiais
def remove_special_characters(text):
    # Usando expressão regular para manter apenas letras, números e espaços
    return re.sub(r'[^a-zA-Z0-9\s]', '', text)

# Defina a tarefa para capturar os dados
>>>>>>> parent of 88f1ffa (Update brt_gps_flow)
@task
def fetch_data():
    try:
        url = "https://dados.mobilidade.rio/gps/brt"
        response = requests.get(url)
<<<<<<< HEAD
        response.raise_for_status()  # Garante que uma exceção seja gerada para códigos de status de erro
=======
        response.raise_for_status()  # Levanta erro para status HTTP não OK
>>>>>>> parent of 88f1ffa (Update brt_gps_flow)
        return response.json()
    except requests.exceptions.RequestException as e:
        raise ValueError(f"Erro ao buscar dados: {e}")

<<<<<<< HEAD
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
        engine = create_engine('postgresql://user:password@localhost:5433/postgres?client_encoding=utf8')
        df.to_sql('brt_gps_data', con=engine, if_exists='append', index=False)
        df.to_sql('brt_gps_data', con=engine, if_exists='append', index=False, method='multi', encoding='utf-8')

    except Exception as e:
        raise ValueError(f"Erro ao carregar dados para o PostgreSQL: {e}")

# Criação do fluxo do Prefect
=======
# Defina a tarefa para processar os dados e salvar em CSV
@task
def process_data(data):
    df = pd.json_normalize(data)
    filename = f"brt_data_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
    try:
        df.to_csv(filename, index=False)
        return filename
    except Exception as e:
        raise ValueError(f"Erro ao salvar o arquivo CSV: {e}")

# Defina a tarefa para carregar os dados no PostgreSQL
@task
def load_to_postgresql(csv_file):
    try:
        # Carregar o CSV com a codificação ISO-8859-1
        df = pd.read_csv(csv_file, encoding='utf-8')
        # Remover caracteres especiais de todas as colunas
        df = df.applymap(lambda x: remove_special_characters(str(x)) if isinstance(x, str) else x)
        
        # Conectar ao PostgreSQL
        engine = create_engine('postgresql://user:password@127.0.0.1:5433/brt_db')
        
        # Carregar o DataFrame no banco de dados
        df.to_sql('brt_gps_data', con=engine, if_exists='append', index=False)
    except Exception as e:
        raise ValueError(f"Erro ao carregar os dados no PostgreSQL: {e}")


# Crie o flow
>>>>>>> parent of 88f1ffa (Update brt_gps_flow)
with Flow("brt_gps_flow") as flow:
    data = fetch_data()
    filename = process_data(data)
    load_to_postgresql(filename)

# Rodando o flow
if __name__ == "__main__":
    flow.run()
