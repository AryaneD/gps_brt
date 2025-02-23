# Usando uma imagem base do Python
FROM python:3.9-slim

# Setando diretório de trabalho
WORKDIR /app

# Copiando os arquivos do projeto para dentro do container
COPY . /app

# Instalando as dependências
RUN pip install --no-cache-dir -r requirements.txt

# Comando para rodar o Prefect flow
CMD ["python", "prefect_flows/brt_gps_flow.py"]
