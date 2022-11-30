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
    logging.info('Python queue trigger function processed a queue item: %s',
                 msg.get_body().decode('utf-8'))

    ticker = msg.get_body().decode("utf-8")
    azure_utils = utils.AzureUtils()
    secret_client = azure_utils.initialize_key_vault()

    sa_secret = azure_utils.get_key_vault_secret(secret_client, 'sa-secret')
    sa_name = azure_utils.get_key_vault_secret(secret_client, 'sa-name')
    azure_utils.initialize_storage_account_ad(sa_secret.value, sa_name.value)

    yahoo_utils = utils.yahooUtils()
    fundamentals = yahoo_utils.get_ticker_financials(ticker)
    fundamentals = fundamentals.astype(str) #Pyarrow has a problem with numpy.dtypes so this is a workaround, hopefully will be fixed in later releases. 
    fundamentals["ObservationDate"] = date.today()
    
    parquet_file = fundamentals.to_parquet(index = False)

    ticker = ticker.replace(".ST", "")
    azure_utils.upload_blob(parquet_file, f"bronze/fundamentals/{date.today()}", f"Fundamentals_{ticker}.parquet")