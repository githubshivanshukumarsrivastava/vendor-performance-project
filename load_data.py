import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time 

# Set up logging


# Make sure 'logs' folder exists
if not os.path.exists('logs'):
    os.makedirs('logs')

logging.basicConfig(filename='logs/ingestion_db.log', level=logging.DEBUG,
                    format='%(asctime)s-%(levelname)s-%(message)s',
                    filemode='a')


#engine= create_engine('sqlite:///data/database.db')
engine = create_engine('postgresql+psycopg2://postgres:root@localhost:5432/vendor_db')
  

'''Ingests CSV files into a database using SQLAlchemy and Pandas.'''
def ingest_db(df,table_name,engine):
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)
    

def load_raw_data():  
    '''Load raw data from CSV files into a database using SQLAlchemy and Pandas.
    This function reads all CSV files from the 'data' directory, logs the ingestion process,'''

    start = time.time()
    for file in os.listdir('data'):
        if '.csv' in file:
            df=pd.read_csv(f'data/{file}')
            logging.info(f'Ingesting {file} into database')
            ingest_db(df,file[:-4],engine)
    end=time.time()
    total_time = (end - start)/60  # Convert to minutes
    logging.info('-------------ingestion completed-----------------')
    logging.info(f'Total time taken for ingestion: {total_time:.2f} minutes')
        
if __name__ == '__main__':
    load_raw_data()
    #logging.info('Data ingestion script executed successfully.')


