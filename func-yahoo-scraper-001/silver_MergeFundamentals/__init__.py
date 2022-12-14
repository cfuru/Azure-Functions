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
    
    selected_cols = [
        'Ticker',
        'ObservationDate',
        'mostRecentQuarter',
        'fullTimeEmployees',
        'ebitdaMargins',
        'profitMargins',
        'grossMargins',
        'operatingCashflow',
        'revenueGrowth',
        'operatingMargins',
        'ebitda',
        'targetLowPrice',
        'recommendationKey',
        'grossProfits',
        'freeCashflow',
        'targetMedianPrice',
        'currentPrice',
        'earningsGrowth',
        'currentRatio',
        'returnOnAssets',
        'numberOfAnalystOpinions',
        'targetMeanPrice',
        'debtToEquity',
        'returnOnEquity',
        'targetHighPrice',
        'totalCash',
        'totalDebt',
        'totalRevenue',
        'recommendationMean',
        'enterpriseToRevenue',
        'enterpriseToEbitda',
        '52WeekChange',
        'morningStarRiskRating',
        'forwardEps',
        'sharesOutstanding',
        'heldPercentInstitutions',
        'trailingEps',
        'lastDividendValue',
        'priceToBook',
        'heldPercentInsiders',
        'beta',
        'enterpriseValue',
        'priceToSalesTrailing12Months',
        'forwardPE',
        'trailingPE',
        'marketCap'
    ]
    cols_to_exclude = [
        'Ticker', 
        'ObservationDate', 
        'recommendationKey', 
        'mostRecentQuarter'
    ]
    
    df_cleaning = (
        azure_utils.ingest_bronze_data(f"fundamentals/{date.today()}")
        .pipe(dataCleaning_utils.pivot_fundamentals_dataframe)
        .pipe(dataCleaning_utils.select_dataframe_columns, selected_cols)
        .pipe(dataCleaning_utils.set_dtype_to_numeric, cols_to_exclude)
        .pipe(dataCleaning_utils.change_timestamp_to_datetime, "mostRecentQuarter")
        .pipe(dataCleaning_utils.change_timestamp_format, "ObservationDate")
    )

    parquet_file = df_cleaning.to_parquet(index = False)

    azure_utils.upload_blob(parquet_file, f"silver/fundamentals", f"fundamentals_{date.today()}.parquet")
    
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
