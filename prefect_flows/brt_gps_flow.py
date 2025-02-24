from prefect import task, Flow
import requests
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, Integer, Float, StringType, DoubleType, LongType
import os

# Inicializando a sessão Spark
spark = SparkSession.builder.appName("BRT Data").getOrCreate()

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

def get_schema():
    return StructType([
        StructField("codigo", StringType(), True),
        StructField("placa", StringType(), True),
        StructField("linha", StringType(), True),
        StructField("latitude", FloatType(), True),
        StructField("longitude", FloatType(), True),
        StructField("dataHora", LongType(), True),  # timestamp em milissegundos
        StructField("velocidade", FloatType(), True),
        StructField("id_migracao_trajeto", StringType(), True),
        StructField("sentido", StringType(), True),
        StructField("trajeto", StringType(), True),
        StructField("hodometro", FloatType(), True),
        StructField("direcao", FloatType(), True),
        StructField("ignicao", IntegerType(), True)
    ])

# Defina a tarefa para processar os dados e salvar em CSV
@task
def process_data(data):
    # Definir o esquema para os dados
    schema = get_schema()
    
    # Criar DataFrame no Spark com base no esquema
    df_spark = spark.read.json(spark.sparkContext.parallelize([data]), schema=schema)
    
    # Converter para Pandas para salvar como CSV
    df = df_spark.toPandas()
    filename = f"brt_data_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
    try:
        df.to_csv(filename, index=False, encoding='utf-8')
        return filename
    except Exception as e:
        raise ValueError(f"Erro ao salvar o arquivo CSV: {e}")

# Defina a tarefa para carregar os dados no PostgreSQL
@task
def load_to_postgresql(csv_file):
    try:
        df = pd.read_csv(csv_file, encoding='utf-8').astype(str)
        engine = create_engine('postgresql://user:password@127.0.0.1:5433/brt_db')
        df.to_sql('brt_gps_data', con=engine, if_exists='append', index=False)
    except Exception as e:
        raise ValueError(f"Erro ao carregar os dados no PostgreSQL: {e}")
    finally:
        engine.dispose()

# Crie o flow
with Flow("brt_gps_flow") as flow:
    data = fetch_data()
    filename = process_data(data)
    load_to_postgresql(filename)

# Rodando o flow
if __name__ == "__main__":
    flow.run()
