from sqlalchemy import create_engine
from time import time
from datetime import datetime
from urllib.request import urlretrieve
import pandas as pd


def download_dataset(url, filepath):
    try:
        # download ny taxi parquet
        urlretrieve(url, filepath)
    except Exception as e:
        print(f"ERROR in downloading the dataset {filepath} from {url}" + str(e))

def ingest_data_callable(user,password,hostname,port,db,table_name,parquet_filename):
    try:
        print(table_name, parquet_filename)
        engine = create_engine(f'postgresql://{user}:{password}@{hostname}:{port}/{db}')
        engine.connect()
        print('Connection established successfully....!!!')

        ### read parquet
        # drop and re-create a table       
        start_time  = time()
        df = pd.read_parquet(parquet_filename, engine='pyarrow')
        # create table structure
        dt = datetime.utcnow().strftime("%m/%d/%Y, %H:%M:%S")
        print('Creating table structure in POSTGRES DB....at {dt}!!!'.format(dt=dt))
        df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        # append data
        df.to_sql(name=table_name, con=engine, if_exists='append')

        end_time = time()
        print(f'inserted data into {table_name}....., took %.3f seconds' %(end_time - start_time))

    except Exception as e:
        print("ERROR in processing data... "+ str(e))
   
    return None