# gps_brt
# Desafio de Engenheiro de Dados

Este projeto consiste em capturar dados da API do BRT, processá-los e armazená-los de forma incremental em uma tabela PostgreSQL. Também são feitas transformações nos dados usando DBT.

## Como Rodar o Projeto

1. Crie e ative um ambiente virtual (opcional):
    ```
    python -m venv venv
    source venv/bin/activate  # Linux/macOS
    venv\Scripts\activate     # Windows
    ```

2. Instale as dependências:
    ```
    pip install -r requirements.txt
    ```

3. Rode o flow do Prefect:
    ```
    python flows/brt_gps_flow.py
    ```

4. Para rodar o banco de dados PostgreSQL, utilize o Docker:
    ```
    docker-compose up
    ```

5. O DBT pode ser rodado para aplicar as transformações.
