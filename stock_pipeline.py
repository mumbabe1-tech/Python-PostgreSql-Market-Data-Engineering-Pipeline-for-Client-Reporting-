# Apex wealth data pipeline
# ETL script is for extraction stock prices from Twelve Data API , transform and load to postgres database.

import requests
import pandas as pd
from dotenv import load_dotenv
import os
import psycopg2
from sqlalchemy import create_engine
import logging

# set up logging

# different levels of logging: info, debug, warning, error 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("pipeline.log"),
        logging.StreamHandler()          
    ]
)
logger = logging.getLogger(__name__)

# configuration
load_dotenv()
API_KEY = os.getenv('API_KEY')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

symbols = [ 'AAPL', 'MSFT', 'GOOGL', 'AMZN' ]

# step 1 - Extract 
def extract(symbols: list) -> list:

    all_records = []

    for symbol in symbols:
        url = f"https://api.twelvedata.com/time_series?symbol={symbol}&interval=1min&apikey={API_KEY}&outputsize=50"
        
        try: 
            response = requests.get(url, timeout=10)
            response.raise_for_status()  # this will raise an error for bad responses
            data = response.json()

            if data.get('status') != 'ok':
                raise ValueError(f'Error fetching data for {symbol}:{data.get("message", "Unknown error")}')

            for record in data["values"]:
                record['symbol'] = symbol

            all_records.extend(data["values"])
            logger.info(f"Extract done for {symbol}: extracted {len(data['values'])} rows")

        except requests.exceptions.RequestException as e:
            logger.error(f"Extract {symbol}: request failed - {e}")
            raise

    logger.info(f"Extract done for all symbols: Extracted {len(all_records)} rows")
    return all_records

# step 2 - transform
def transform_data(records: list) -> pd.DataFrame:

    try:
        # convert to dataframe
        df = pd.DataFrame(records)

        # convert to proper data types
        df['datetime'] = pd.to_datetime(df['datetime'])

        df = df.astype({
            'open': 'float',
            'high': 'float',
            'low': 'float',
            'close': 'float',
            'volume': 'int'
        })

        logger.info(f"Transform done-{len(df)} rows")
        return df

    except Exception as e:
        logger.error(f"Transform failed: {e}")
        raise

# step 3 - load
def load(df: pd.DataFrame) -> None:

    try:
        db_url = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
        engine = create_engine(db_url)

        # load dataframe to postgres database
        df.to_sql('stockPrices_data', engine, if_exists='append', index=False)

        # save to csv file
        df.to_csv('stock_add_symbol_data.csv', index=False)

        logger.info(f"Load done - {(len(df))} rows loaded into database")

    except Exception as e:
        logger.error(f"load failed - {e}")
        raise

def run_pipeline():

    logger.info("pipeline started")
    
    records = extract(symbols)
    df = transform_data(records)
    load(df)

    logger.info("pipeline finished successfully")

if __name__ == "__main__":
    run_pipeline()