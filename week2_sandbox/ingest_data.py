#!/usr/bin/env python
# coding: utf-8

import os
from datetime import timedelta
import pandas as pd
from sqlalchemy import create_engine
from prefect import flow, task
from prefect.tasks import task_input_hash 
from prefect_sqlalchemy import SqlAlchemyConnector

@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data(url):
    output_name = url.split('/')[-1]
    os.system(f"wget {url} -O {output_name}")
    df = pd.read_parquet(output_name)
    return df

@task(log_prints=True)
def transform_data(df):
    print(f"pre:missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    df = df[df['passenger_count'] != 0]
    print(f"post:missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    return df

@task(log_prints=True, retries=3)
def ingest_data(table_name, df):
    connection_block = SqlAlchemyConnector.load("postgres-connector")
    with connection_block.get_connection(begin=False) as engine:
        # Clear table if exists
        df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
        df.to_sql(name=table_name, con=engine, if_exists='append')
    
@flow(name="Subflow", log_prints=True)
def log_subflow(table_name:str):
    print(f"Logging Subflow for: {table_name}")

@flow(name="Ingest Flow")
def main_flow(table_name: str):
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet"

    log_subflow(table_name)

    raw_data = extract_data(url)
    data = transform_data(raw_data)

    ingest_data(table_name, data)

if __name__ == '__main__':
    main_flow("yellow_taxi_data")
