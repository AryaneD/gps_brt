from prefect import Flow
from fetch import fetch_data
from process import process_data
from load import load_to_postgresql

# Crie o fluxo do Prefect
with Flow("brt_gps_flow") as flow:
    data = fetch_data()
    filename = process_data(data)
    load_to_postgresql(filename)

# Rodando o fluxo
if __name__ == "__main__":
    flow.run()
