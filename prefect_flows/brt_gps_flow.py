from prefect import task, Flow
import requests
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
import chardet
import logging

# Configurar logger para monitoramento
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Função para detectar a codificação do arquivo
def detect_encoding(file_path):
    with open(file_path, 'rb') as f:
        result = chardet.detect(f.read())
    return result['encoding']

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
    df.to_csv(filename, index=False, encoding="utf-8")  # Garantir que o arquivo seja salvo em UTF-8
    return filename

# Defina a tarefa para carregar os dados no PostgreSQL
@task
def load_to_postgresql(csv_file):
    try:
        encoding = detect_encoding(csv_file)  # Detectar a codificação do arquivo
        df = pd.read_csv(csv_file, encoding=encoding)  # Ler o arquivo com a codificação detectada
        
        # Limpar caracteres problemáticos, se necessário
        df = df.applymap(lambda x: x.encode("utf-8", "ignore").decode("utf-8") if isinstance(x, str) else x)
        
        # Criar a conexão com o PostgreSQL
        engine = create_engine('postgresql://user:password@localhost:5433/brt_db')
        
        # Carregar os dados no banco de dados
        df.to_sql('brt_gps_data', con=engine, if_exists='append', index=False)
        logger.info(f"Dados carregados com sucesso no banco de dados PostgreSQL.")
    
    except UnicodeDecodeError as e:
        logger.error(f"Erro de codificação ao ler o arquivo {csv_file}: {e}")
        raise
    except Exception as e:
        logger.error(f"Erro ao carregar os dados no PostgreSQL: {e}")
        raise

# Crie o flow
with Flow("brt_gps_flow") as flow:
    data = fetch_data()
    filename = process_data(data)
    load_to_postgresql(filename)

# Rodando o flow
if __name__ == "__main__":
    flow.run()
