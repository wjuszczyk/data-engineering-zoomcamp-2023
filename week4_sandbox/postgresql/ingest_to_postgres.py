#!/usr/bin/env python
# coding: utf-8
"""Ingest data stored in local parquet file to PostgreSQL DB."""

import argparse
#import os
from pathlib import Path
from time import time
import pyarrow.parquet as pq
import pandas as pd

from sqlalchemy import create_engine


def main(params):
    """Main function"""
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    database = params.db
    table_name = params.table_name

    engine = create_engine(
        f'postgresql://{user}:{password}@{host}:{port}/{database}')

    path = Path(params.path).glob('**/*')
    list_of_files = [x for x in path if x.is_file()]

    f_index = 1

    for file in list_of_files:
        print(f"* file: {file.resolve()} ({file.stat().st_size / 1024 / 1024:.2f} MB) ({f_index}/{len(list_of_files)}) -> DB: {database}, schema: {table_name}\n")

        t_start = time()

        if file.name.endswith('parquet'):
            parquet_file = pq.ParquetFile(file)
            parquet_size = parquet_file.metadata.num_rows
            index = 65536

            for iterator in parquet_file.iter_batches(use_threads=True):
                if index > parquet_size:
                    index = parquet_size

                data_frame = iterator.to_pandas()

                if "green" in file.name:
                    data_frame['lpep_pickup_datetime'] = pd.to_datetime(
                        data_frame['lpep_pickup_datetime'])
                    data_frame['lpep_dropoff_datetime'] = pd.to_datetime(
                        data_frame['lpep_dropoff_datetime'])
                    data_frame["trip_type"] = data_frame["trip_type"].astype(
                        'Int64')
                    data_frame["VendorID"] = data_frame["VendorID"].astype(
                        'Int64')
                    data_frame["RatecodeID"] = data_frame["RatecodeID"].astype(
                        'Int64')
                    data_frame["PULocationID"] = data_frame["PULocationID"].astype(
                        'Int64')
                    data_frame["DOLocationID"] = data_frame["DOLocationID"].astype(
                        'Int64')
                    data_frame["passenger_count"] = data_frame["passenger_count"].astype(
                        'Int64')
                    data_frame["payment_type"] = data_frame["payment_type"].astype(
                        'Int64')

                if "yellow" in file.name:
                    data_frame['tpep_pickup_datetime'] = pd.to_datetime(
                        data_frame['tpep_pickup_datetime'])
                    data_frame['tpep_dropoff_datetime'] = pd.to_datetime(
                        data_frame['tpep_dropoff_datetime'])
                    data_frame["VendorID"] = data_frame["VendorID"].astype(
                        'Int64')
                    data_frame["RatecodeID"] = data_frame["RatecodeID"].astype(
                        'Int64')
                    data_frame["PULocationID"] = data_frame["PULocationID"].astype(
                        'Int64')
                    data_frame["DOLocationID"] = data_frame["DOLocationID"].astype(
                        'Int64')
                    data_frame["passenger_count"] = data_frame["passenger_count"].astype(
                        'Int64')
                    data_frame["payment_type"] = data_frame["payment_type"].astype(
                        'Int64')

                print(
                    f"Ingesting {index} out of {parquet_size} rows")
                data_frame.to_sql(name=table_name, con=engine,
                                  if_exists='append')
                index += 65535
            print(f"* Ingested, it took {time() - t_start:.1f} seconds\n")

        elif file.name.endswith("csv.gz"):
            chunksize = 100000
            index = chunksize
            csv_size = pd.read_csv(file).shape[0]
            df_iter = pd.read_csv(file, iterator=True,
                                  chunksize=chunksize, low_memory=False)
            data_frame = next(df_iter)

            data_frame.to_sql(name=table_name, con=engine, if_exists='append')

            while True:
                try:
                    if index > csv_size:
                        index = csv_size
                    print(
                        f"Ingesting {index} out of {csv_size} rows")
                    data_frame = next(df_iter)
                    data_frame.to_sql(
                        name=table_name, con=engine, if_exists='append')
                    index += chunksize
                except StopIteration:
                    print('Reached end of csv file.')
                    break
            print(f"* Ingested, it took {time() - t_start:.1f} seconds\n")
        f_index += 1


if __name__ == '__main__':
    DB = "taxi_rides_all"
    parser = argparse.ArgumentParser(
        description='Ingest local parquet data to PostgreSQL')

    parser.add_argument('--user', default='root',
                        help='username for PostgreSQL (default: root)')
    parser.add_argument('--password', default='root',
                        help='password for PostgreSQL (default: root)')
    parser.add_argument('--host', default='localhost',
                        help='hostname for PostgreSQL (default: localhost)')
    parser.add_argument('--port', type=int, default=5432,
                        help='port for PostgreSQL (default: 5432)')
    parser.add_argument('--db', default=DB,
                        help='database name for PostgreSQL (default: {DB})')
    parser.add_argument('--table_name', required=True,
                        help='name of the table')
    parser.add_argument('--path', required=True, help='Path to parquet files.')
    args = parser.parse_args()

    main(args)
