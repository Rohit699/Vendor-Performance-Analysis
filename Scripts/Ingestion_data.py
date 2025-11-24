import pandas as pd
from sqlalchemy import create_engine
import os
import logging
import time

logging.basicConfig(
    filename = "Logs/ingestion_db.logs",
    level = logging.DEBUG,
    format = "%(asctime)s - %(levelname)s - %(message)s",
    filemode ='a'
)

engine = create_engine('sqlite:///inventory.db')


''' This function ingest the dataframe into db tables '''
def ingest_data( df , table_name , engine):
    df.to_sql(table_name , con= engine , if_exists='replace', index=False)



''' This function will load the csv as dataframe and ingest the data in db'''
def ingestion_raw_data():
    start = time.time()
    logging.info('-------------------Ingestion Start-------------------')
    for file in os.listdir('VPA'):
        if '.csv' in file:
            df = pd.read_csv('VPA/'+file)
            logging.info(f'Ingestion {file} data in db')
            ingest_data(df , file[:-4] , engine)
    end = time.time()
    total_time = (end-start)/60
    logging.debug('-------------------Ingestion Compelete--------------')
    logging.debug(f'Total time taken to ingest dataframe in db is {total_time} minutes')              
        
        
if __name__ == "__main__":
    ingestion_raw_data()
    
