from prefect import task, flow
import requests
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
import shutil
import os

# Defina a tarefa para capturar os dados da API
@task
def fetch_data():
    url = "https://dados.mobilidade.rio/gps/brt"
    response = requests.get(url)
    return response.json()

# Defina a tarefa para processar os dados e salvar em CSV incremental
@task
def process_data(data):
    df = pd.json_normalize(data)
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    filename = f"brt_data_{timestamp}.csv"
    df.to_csv(filename, index=False)
    return filename

# Defina a tarefa para mover o CSV para o Docker (diretório compartilhado)
@task
def move_to_docker(csv_file):
    docker_directory = '/path/to/docker/volume/brt_data/'  # Caminho no volume ou diretório do container
    if not os.path.exists(docker_directory):
        os.makedirs(docker_directory)  # Cria o diretório se não existir
    shutil.move(csv_file, os.path.join(docker_directory, os.path.basename(csv_file)))
    return os.path.join(docker_directory, os.path.basename(csv_file))

# Defina a tarefa para carregar os dados no PostgreSQL
@task
def load_to_postgresql(csv_file):
    df = pd.read_csv(csv_file)
    engine = create_engine('postgresql://postgres:password@localhost:5433/postgres')  # Atualize com as credenciais
    df.to_sql('brt_gps_data', con=engine, if_exists='append', index=False)

# Defina o fluxo
@flow
def brt_gps_flow():
    # Etapa 1: Capturar os dados da API
    data = fetch_data()
    
    # Etapa 2: Processar os dados e salvar no CSV
    filename = process_data(data)
    
    # Etapa 3: Mover o CSV para o Docker
    csv_in_docker = move_to_docker(filename)
    
    # Etapa 4: Carregar os dados no PostgreSQL
    load_to_postgresql(csv_in_docker)

# Rodando o fluxo
if __name__ == "__main__":
    brt_gps_flow()
