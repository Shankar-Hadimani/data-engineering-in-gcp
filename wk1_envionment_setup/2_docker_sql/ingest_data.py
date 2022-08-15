from operator import index
from textwrap import indent
from sqlalchemy import create_engine
from time import time
import pyarrow
import urllib.request
import psycopg2
import pandas as pd
import argparse


def main(params):
   user  = params.user
   password = params.password
   hostname = params.hostname
   port = params.port
   db = params.db
   table_name = params.table_name
   url = params.url

   try:
      # download ny taxi parquet
      filename = 'yellow_tripdata_2021-01.parquet'
      urllib.request.urlretrieve(url, filename)

      # configure jdbc connection string engine
      engine = create_engine(f'postgresql://{user}:{password}@{hostname}:{port}/{db}')

      ### read paruet
      df_tripdata = pd.read_parquet(filename, engine='pyarrow', index=False)
      df = df_tripdata.head(10000)
      df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
      df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

      # engine.connect()

      # drop and re-create a table
      start_time  = time()
      df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

      # append data 
      df.to_sql(name=table_name, con=engine, if_exists='append')

      end_time = time()
      print(f'inserted data into {table_name}....., took %.3f seconds' %(end_time - start_time))
   except Exception as e:
      print("ERROR in processing data... "+ str(e))
   
   return None


if __name__ =='__main__':
   ### set descriptin for argiument parser
   parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres.')
   
   ### add arguments
   parser.add_argument('--user', required=True, help='logon user from postgres')
   parser.add_argument('--password',required=True,  help='password for postgres')
   parser.add_argument('--hostname', required=True, help='Hostname for postgres')
   parser.add_argument('--port', required=True, help='Port number for postgres')
   parser.add_argument('--db', required=True, help='Database name in postgres')
   parser.add_argument('--table_name', required=True, help='Destination table name')
   parser.add_argument('--url', required=True, help='Url of the csv file')

   args = parser.parse_args()
   
   main(args)