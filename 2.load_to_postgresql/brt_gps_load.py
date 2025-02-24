import pandas as pd
from sqlalchemy import create_engine
from prefect import task

@task
def load_to_postgresql(csv_file):
    df = pd.read_csv(csv_file)
    engine = create_engine('postgresql://user:password@localhost:5433/postgres')
    df.to_sql('brt_gps_data', con=engine, if_exists='append', index=False)
