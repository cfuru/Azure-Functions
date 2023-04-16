#sys.path.insert(0, 'C:/Users/chris/Documents/GitHub/AzureFunctions/AzureFunctions')
import datetime
import logging
import time
from datetime import date
import os
from io import BytesIO
import pandas as pd

import azure.functions as func
from shared_code import utils

def main(mytimer: func.TimerRequest) -> None:

    azure_utils = utils.AzureUtils()
    dataCleaning_utils = utils.DataCleaning()
    
    secret_client = azure_utils.initialize_key_vault()
    sa_secret = azure_utils.get_key_vault_secret(secret_client, 'sa-secret')
    sa_name = azure_utils.get_key_vault_secret(secret_client, 'sa-name')
    azure_utils.initialize_storage_account_ad(sa_secret.value, sa_name.value)
    
    dim_company = azure_utils.download_parquet_blob(f"gold/dimcompany", f"dim_company.parquet")
        
    df_company = (
        azure_utils.ingest_silver_data(f"company/company_")
    )
    
    dim_company = dim_company.set_index("Ticker")
    df_company = df_company.set_index("Ticker")
    dim_company = pd.concat([df_company[~df_company.index.isin(dim_company.index)], dim_company]).reset_index()
    
    parquet_file = dim_company.to_parquet(index = False)

    azure_utils.upload_blob(parquet_file, "gold/dimcompany", "dim_company.parquet")
    
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
