from prefect import Flow, task
import requests
import pandas as pd
from sqlalchemy import create_engine
from prefect.schedules import IntervalSchedule

# Schedule: Execute a cada minuto
schedule = IntervalSchedule(interval=pd.Timedelta(minutes=1))

# Extração de dados diretamente da API
@task
def fetch_data():
    try:
        url = "https://dados.mobilidade.rio/gps/brt"
        response = requests.get(url)
        response.raise_for_status()  # Garante que uma exceção seja gerada para códigos de status de erro
        return response.json()
    except requests.exceptions.RequestException as e:
        raise ValueError(f"Erro ao buscar dados: {e}")

# Carregamento incremental para o PostgreSQL
@task
def load_to_postgresql(data):
    try:
        # Normalizando os dados JSON para um DataFrame
        df = pd.json_normalize(data)
        
        # Criar a conexão com o PostgreSQL usando SQLAlchemy
        engine = create_engine('postgresql://user:password@localhost:5433/postgres?client_encoding=utf8')
        
        # Carregar os dados para a tabela do PostgreSQL (incremental)
        df.to_sql('brt_gps_data', con=engine, if_exists='append', index=False, method='multi', encoding='utf-8')

    except Exception as e:
        raise ValueError(f"Erro ao carregar dados para o PostgreSQL: {e}")

# Criação do fluxo do Prefect
with Flow("brt_gps_flow", schedule=schedule) as flow:
    # Passo de extração
    data = fetch_data()
    
    # Passo de carregamento para o banco de dados
    load_to_postgresql(data)

# Rodando o fluxo
if __name__ == "__main__":
    flow.run()
