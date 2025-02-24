import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
import os
import re

# Função para remover caracteres especiais
def remove_special_characters(text):
    # Usando expressão regular para manter apenas letras, números e espaços
    return re.sub(r'[^a-zA-Z0-9\s]', '', text)

# Defina a tarefa para capturar os dados
@task
def fetch_data():
    url = "https://dados.mobilidade.rio/gps/brt"
    try:
        response = requests.get(url)
        response.raise_for_status()  # Levanta erro para status HTTP não OK
        return response.json()
    except requests.exceptions.RequestException as e:
        raise ValueError(f"Erro ao obter dados da API: {e}")

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
        #df = pd.read_csv(csv_file)
        # Carregar o CSV com a codificação ISO-8859-1
        df = pd.read_csv(csv_file, encoding='ISO-8859-1')
        
        # Remover caracteres especiais de todas as colunas
        df = df.applymap(lambda x: remove_special_characters(str(x)) if isinstance(x, str) else x)
        
        # Conectar ao PostgreSQL
        engine = create_engine('postgresql://user:password@127.0.0.1:5433/brt_db')
        # Especificando os tipos de dados de acordo com o DataFrame
        
        # Carregar o DataFrame no banco de dados
        df.to_sql('brt_gps_data', con=engine, if_exists='append', index=False)
    except Exception as e:
        raise ValueError(f"Erro ao carregar os dados no PostgreSQL: {e}")
    finally:
        # Fechar a conexão com o banco de dados, se necessário
        engine.dispose()

# Crie o flow
with Flow("brt_gps_flow") as flow:
    data = fetch_data()
    filename = process_data(data)
    load_to_postgresql(filename)

# Rodando o flow
if __name__ == "__main__":
    flow.run()