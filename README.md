# gps_brt
# Desafio de Engenheiro de Dados

Este projeto consiste em capturar dados da API do BRT, processá-los e armazená-los de forma incremental em uma tabela PostgreSQL. Também são feitas transformações nos dados usando DBT.

## Como Rodar o Projeto

1. Instale as dependências:
    ```
    pip install -r requirements
    ```
2. Configure um banco de dados PostgreSQL e utilize o Docker:
    ```
    docker-compose up
    ```

3. Rode o flow do Prefect:
    ```
    python prefect_flows/brt_gps_flow.py
    ```

4. O DBT pode ser rodado para aplicar as transformações.
    ```
    dbt run
    ```
