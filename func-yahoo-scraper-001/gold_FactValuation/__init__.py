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
    
    fact_valuation = azure_utils.download_parquet_blob(f"gold/factvaluation", f"fact_valuation.parquet")
    
    selected_cols = [
        "Ticker",
        "ObservationDate",
        "mostRecentQuarter",
        "enterpriseToRevenue",
        "enterpriseToEbitda",
        "forwardEps",
        "trailingEps",
        "enterpriseValue",
        "forwardPE",
        "trailingPE",
        "marketCap"
    ]
    
    df_valuation = (
        azure_utils.ingest_silver_data(f"fundamentals/fundamentals_")
        .pipe(dataCleaning_utils.select_dataframe_columns, selected_cols)
    )
    
    fact_valuation = fact_valuation.set_index(["Ticker", "ObservationDate"])
    df_valuation = df_valuation.set_index(["Ticker", "ObservationDate"])
    fact_valuation = pd.concat([df_valuation[~df_valuation.index.isin(fact_valuation.index)], fact_valuation]).reset_index()

    parquet_file = fact_valuation.to_parquet(index = False)

    azure_utils.upload_blob(parquet_file, f"gold/factvaluation", f"fact_valuation.parquet")
    
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
