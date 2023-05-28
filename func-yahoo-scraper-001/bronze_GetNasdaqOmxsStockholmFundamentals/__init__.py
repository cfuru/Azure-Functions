import datetime
import logging
import time
from datetime import date
import os
from io import BytesIO
import pandas as pd

import azure.functions as func
from shared_code import utils

def main(msg: func.QueueMessage) -> None:
    
    try:
        ticker = msg.get_body().decode("utf-8")
        logging.info('Python queue trigger function processed a queue item: %s', ticker)

        # Initialize Azure Utils
        azure_utils = utils.AzureUtils()
        secret_client = azure_utils.initialize_key_vault()

        sa_secret = os.getenv("AZURE_STORAGE_SECRET")#azure_utils.get_key_vault_secret(secret_client, 'sa-secret')
        sa_name = os.getenv("AZURE_STORAGE_NAME")#azure_utils.get_key_vault_secret(secret_client, 'sa-name')
        azure_utils.initialize_storage_account_ad(sa_secret, sa_name)

        # Get financials data and upload to Blob Storage
        fetch_and_upload_financials(ticker, azure_utils)

    except Exception as e:
        logging.error(f'Error occurred: {str(e)}')
        
def fetch_and_upload_financials(ticker, azure_utils):
    """Fetch financials data using yfinance and upload to Azure Blob Storage."""
    try:
        yahoo_utils = utils.yahooUtils()
        fundamentals = yahoo_utils.get_ticker_financials(ticker)
        fundamentals = fundamentals.astype(str)  # Pyarrow has a problem with numpy.dtypes so this is a workaround, hopefully will be fixed in later releases. 
        fundamentals["ObservationDate"] = date.today()

        parquet_file = fundamentals.to_parquet(index = False)

        ticker = ticker.replace(".ST", "")
        azure_utils.upload_blob(parquet_file, f"bronze/fundamentals/NasdaqOmxsStockholm/{date.today()}", f"Fundamentals_{ticker}.parquet")

    except Exception as e:
        logging.error(f'Error occurred while fetching and uploading financials data: {str(e)}')
