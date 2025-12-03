import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time

'''Setup logging'''
logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

'''MySQL connection(update with your credentials)'''
'''Format: mysql+pymysql://username(root):password(Sanjana@1611)@host:port/database'''

engine = create_engine("mysql+pymysql://root:Sanjana%401611@localhost:3306/inventorydb")

def ingest_db(df, table_name, engine):
    '''this function will ingest the dataframe into database table'''
    df.to_sql(table_name, con = engine, if_exists = 'replace', index = False)
    logging.info(f"Successfully ingested {table_name} into database")
    
def load_raw_data():
    '''this function will load the CSVs as dataframe and ingest into db'''
    start = time.time()
    for file in os.listdir('documents/vendors/data/data/'):
        if '.csv' in file:
            df = pd.read_csv('documents/vendors/data/data/'+file)
            logging.info(f'Ingesting {file} in db')
            ingest_db(df, file[:-4], engine)
    end = time.time()
    total_time = (end - start)/60
    logging.info('--------------Ingestion Complete-------------')
    logging.info(f'\nTotal Time Taken: {total_time} minutes')

if __name__ == '__main__':
    load_raw_data()