#!/usr/bin/env python
# coding: utf-8
"""Ingest data stored in csv or parquet file to PostgreSQL DB."""

import argparse
import os
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
    url = params.url

    engine = create_engine(
        f'postgresql://{user}:{password}@{host}:{port}/{database}')

    output_name = url.split('/')[-1]

    os.system(f"wget {url} -O {output_name}")

    if url.endswith('parquet'):
        parquet_file = pq.ParquetFile(output_name)
        parquet_size = parquet_file.metadata.num_rows

        # Clear table if exists
        pq.read_table(output_name).to_pandas().head(n=0).to_sql(
            name=table_name, con=engine, if_exists='replace')

        index = 65536

        for i in parquet_file.iter_batches(use_threads=True):
            t_start = time()
            print(
                f'Ingesting {index} out of {parquet_size} rows ({index / parquet_size:.0%})')
            i.to_pandas().to_sql(name=table_name, con=engine, if_exists='append')
            index += 65536
            t_end = time()
            #print('\t- it took %.1f seconds' % (t_end - t_start))
            print(
                f"\t- it took {time:.1f} seconds".format(time=t_end - t_start))
    elif url.endswith('.csv') or url.endswith('.csv.gz'):
        if url.endswith('.csv.gz'):
            os.system(f"gunzip -f {output_name}")
            output_name = output_name.rstrip('.gz')

        df_iter = pd.read_csv(output_name, iterator=True,
                              chunksize=100000, low_memory=False)
        data_frame = next(df_iter)

        data_frame.lpep_pickup_datetime = pd.to_datetime(
            data_frame.lpep_pickup_datetime)
        data_frame.lpep_dropoff_datetime = pd.to_datetime(
            data_frame.lpep_dropoff_datetime)

        data_frame.head(n=0).to_sql(name=table_name,
                                    con=engine, if_exists='replace')
        data_frame.to_sql(name=table_name, con=engine, if_exists='append')

        while True:
            try:
                t_start = time()
                data_frame = next(df_iter)
                data_frame.lpep_pickup_datetime = pd.to_datetime(
                    data_frame.lpep_pickup_datetime)
                data_frame.lpep_dropoff_datetime = pd.to_datetime(
                    data_frame.lpep_dropoff_datetime)
                data_frame.to_sql(name=table_name, con=engine,
                                  if_exists='append')
                t_end = time()
                print(f"inserted another chunk, took {time:.3f} seconds".format(
                    time=t_end - t_start))
            except StopIteration:
                print('Reached end of csv file.')
                break
    else:
        print("Wrong URL!")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Ingest CSV data to PostgreSQL')

    parser.add_argument('--user', required=True,
                        help='username for PostgreSQL')
    parser.add_argument('--password', required=True,
                        help='password for PostgreSQL')
    parser.add_argument('--host', required=True,
                        help='hostname for PostgreSQL')
    parser.add_argument('--port', required=True, type=int,
                        help='port for PostgreSQL')
    parser.add_argument('--db', required=True,
                        help='database name for PostgreSQL')
    parser.add_argument('--table_name', required=True,
                        help='name of the table')
    parser.add_argument('--url', required=True, help='url of csv file')

    args = parser.parse_args()

    main(args)
