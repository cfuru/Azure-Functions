#sys.path.insert(0, 'C:/Users/chris/Documents/GitHub/AzureFunctions/AzureFunctions')
import datetime
import logging
import time
import os
from io import BytesIO
from datetime import date
import pandas as pd

import azure.functions as func
from shared_code import utils

def main(mytimer: func.TimerRequest) -> None:
    
    azure_utils = utils.AzureUtils()
    secret_client = azure_utils.initialize_key_vault()
    
    sa_secret = azure_utils.get_key_vault_secret(secret_client, 'sa-secret')
    sa_name = azure_utils.get_key_vault_secret(secret_client, 'sa-name')
    connect_str = azure_utils.get_key_vault_secret(secret_client, 'sa-conn-str')

    azure_utils.initialize_storage_account_ad(sa_secret.value, sa_name.value)
    azure_utils.initialize_queue_client(connect_str.value, "processing")

    yahoo_utils = utils.yahooUtils()
    companies = yahoo_utils.scrape_nasdaq_companies()
    
    for ticker in companies.tickers:
        azure_utils.send_queue_message(ticker)
    
    parquet_file = companies.to_parquet(index = False)
    azure_utils.upload_blob(parquet_file, "bronze/companies/nasdaqOmxStockholm", "companies.parquet")

    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
