#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
from time import time
import os
import argparse

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    csv_compr_name = 'output.csv.gz' # I modified this because in the lesson video the file was a csv format. Here it has a gz compression
    csv_name = 'output.csv'

    # Download csv
    os.system(f'wget {url} -O {csv_compr_name}')
    os.system(f'gzip -d {csv_compr_name}') # I modified this because in the lesson video the file was a csv format. Here it has a gz compression

    # Load data
    df = pd.read_csv(csv_name, nrows=100)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    # Create engine
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    # Create df iterable
    df_iter = pd.read_csv(csv_name, low_memory=False, chunksize=100000) #add low_memory arg because a dtype error in col6.

    # Create head of DB
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace') # Load the header and then append the data in loop for chunked df

    # Upload data on DB
    while True:
        t_start = time()

        df = next(df_iter)

        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        df.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()

        print('inserted another chunk..., took %.3f seconds' % (t_end - t_start))


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write the results to')
    parser.add_argument('--url', help='url of the csv file')

    args = parser.parse_args()

    main(args)




