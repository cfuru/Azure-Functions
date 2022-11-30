import queue
import pandas as pd
import logging
import requests as r
import yfinance as yf

from lxml import html
from io import BytesIO
from io import StringIO
from datetime import date
from azure.storage.blob import BlobServiceClient
from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.queue import (
        QueueClient,
        BinaryBase64EncodePolicy,
        BinaryBase64DecodePolicy
)

class AzureUtils:
    def __init__(self, vault_url=None, storageaccount=None):
        self.vault_url = "https://kv-yahoo-prod-001.vault.azure.net/"

    def initialize_storage_account_ad(self, storage_account_secret, blob):
        try:  
            global blob_service_client_instance
            blob_service_client_instance = BlobServiceClient(
                account_url = "{}://{}.blob.core.windows.net".format("https", blob), 
                credential = storage_account_secret
                )
        except Exception as e:
            logging.error(f"Could not create blob service client: {e}")
            
    def initialize_data_lake(self, storage_account_name, storage_account_secret):
        try:  
            global datalake_service_client
            datalake_service_client = DataLakeServiceClient(
                account_url = "{}://{}.dfs.core.windows.net".format("https", storage_account_name), 
                credential = storage_account_secret
                )
        except Exception as e:
            logging.error(f"Could not create data lake service client: {e}")

    def initialize_queue_client(self, accountUrl, queueName):
        try:  
            global queue_client_instance
            queue_client_instance = QueueClient.from_connection_string(
                conn_str = accountUrl, 
                queue_name = queueName,
                message_encode_policy = BinaryBase64EncodePolicy(),
                message_decode_policy = BinaryBase64DecodePolicy()
                )
            logging.info(f"Connected to queue {queueName} successfully")
        except Exception as e:
            logging.error(f"Error connecting to queue {queueName}: {e}")

    def send_queue_message(self, message):
        try:
            queue_client_instance.send_message(message.encode("utf-8"))
            logging.info(f"Queued message {message} successfully")
        except Exception as e:
            logging.error(f"Error queueing message {message}: {e}")
   
    def upload_blob(self, data, container, blob_name):
        blob_client_instance = blob_service_client_instance.get_blob_client(container, blob_name, snapshot = None)
        try:
            blob_client_instance.upload_blob(data, overwrite = True, encoding = "utf-8", length=len(data))
            logging.info(f"Created blob {blob_name} successfully")
        except Exception as e:
            logging.error(f"Error creating blob {blob_name}: {e}")
            
    def write_dataframe_to_datalake(self, df, dir_name, filename):
        file_system_client = datalake_service_client.get_file_system_client(file_system = "gold")
        directory_client = file_system_client.get_directory_client(dir_name)
        file_client = directory_client.create_file(f'{filename}_{date.today()}.Parquet')
        
        df_parquet = df.to_parquet()
        file_client.append_data(data = df_parquet, offset = 0, length = len(df_parquet))
        file_client.flush_data(len(df_parquet))
        return True
    
    def download_parquet_blob(self, container, blob_name):
        blob_client_instance = blob_service_client_instance.get_blob_client(container, blob_name, snapshot = None)
        try:
            with BytesIO() as input_blob:
                blob_client_instance.download_blob().download_to_stream(input_blob)
                input_blob.seek(0)
                df = pd.read_parquet(input_blob)
            logging.info(f"Downloaded blob {blob_name} successfully")    
        except Exception as e:
            logging.error(f"Error downloadning blob {blob_name}: {e}")
        return df
    
    def download_csv_blob(self, container, blob_name):
        blob_client_instance = blob_service_client_instance.get_blob_client(container, blob_name, snapshot = None)
        try:
            with BytesIO() as input_blob:
                blob_client_instance.download_blob().download_to_stream(input_blob)
                input_blob.seek(0)
                df = pd.read_csv(input_blob)
            logging.info(f"Downloaded blob {blob_name} successfully")    
        except Exception as e:
            logging.error(f"Error downloadning blob {blob_name}: {e}")
        return df
    
    def ingest_bronze_data(self, directory):
        blob_list = self.list_blobs("bronze", directory)
        df = pd.concat([self.download_parquet_blob("bronze", blob.name) for blob in blob_list], ignore_index = True)
        return df
    
    def list_blobs(self, container, blob_name_starts_with):
        try:
            container_client_instance = blob_service_client_instance.get_container_client(container)
            blob_list = container_client_instance.list_blobs(blob_name_starts_with)
            logging.info(f"Retreived list of blobs that starts with name {blob_name_starts_with} from container {container}")
        except Exception as e:
            logging.error(f"Error retreiving list of blobs in container {container} with blob name starting with {blob_name_starts_with}: {e}")
        return blob_list
            
    def initialize_key_vault(self):
        credential = DefaultAzureCredential(additionally_allowed_tenants=['*'])
        secret_client = SecretClient(vault_url = self.vault_url, credential=credential)
        return secret_client

    def get_key_vault_secret(self, secret_client, secret_name):
        return secret_client.get_secret(secret_name)

class yahooUtils:
    def __init__(self):
        self.nasdaq_base_url = "http://www.nasdaqomxnordic.com" 
        self.nasdaq_full_url = self.nasdaq_base_url + "/shares/listed-companies/stockholm"

    def scrape_nasdaq_companies(self):
        headers = {"User-Agent": "Mozilla/5.0 (X11; CrOS x86_64 12871.102.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.141 Safari/537.36"}
        page = r.get(self.nasdaq_full_url, headers = headers)
        tree = html.fromstring(page.content)
        page.close
        tree.make_links_absolute(self.nasdaq_base_url)
        trs = tree.xpath('//tbody//tr')
        df_companies = pd.DataFrame(
                [[j.text_content() for j in i.getchildren()[:-1]] for i in trs],
                columns = ['name','symbol','currency','isin','sector','icb']
                )
        # here we create the ticker names for the queries to Yahoo Finance
        df_companies['tickers'] = ["-".join(i.split(" "))+".ST" for i in df_companies['symbol'].values]
        return df_companies

    def get_ticker_financials(self, ticker):
        try:
            ticker_object = yf.Ticker(ticker)
            temp = pd.DataFrame.from_dict(ticker_object.info, orient = "index")
            temp.reset_index(inplace = True)
            temp.columns = ["Attribute", "Recent"]
            temp["Ticker"] = ticker
        except Exception as e:
            print(e)
        return temp

#Structure created by Sarah Floris
class DataCleaning:
    def __init__(self):
        pass
    
    def pivot_fundamentals_dataframe(self, df, selected_index = ["Ticker", "ObservationDate"], selected_column = "Attribute", selected_value = "Recent"):
        try:
            return df.pivot(index = selected_index, columns = selected_column, values = selected_value).reset_index()
        except Exception as e:
            logging.error(f"Couldn't pivot the dataframe: {e}")
    
    def select_dataframe_columns(self, df, columns):
        try:
            return df[columns]
        except Exception as e:
            logging.error(f"Could not select columns {columns}: {e}")
            
    def set_dtype_to_numeric(self, df, cols_to_exclude):
        try:
            df.loc[:, ~df.columns.isin(cols_to_exclude)] = df.loc[:, ~df.columns.isin(cols_to_exclude)].apply(pd.to_numeric, errors = 'coerce')
        except Exception as e:
            logging.error(f"Could not change data type to numeric for all columns except {cols_to_exclude}: {e}")
        return df
    
    def change_timestamp_to_datetime(self, df, column_name):
        try:
            df[column_name] = df[column_name].apply(lambda x: pd.to_datetime(x, unit = 's'))
        except Exception as e:
            logging.error(f"Could not change timestamp to datetime for column {column_name}: {e}")
        return df
    
    def change_timestamp_format(self, df, column_name, date_format = '%Y-%m-%d'):
        df[column_name] = df[column_name].apply(lambda x: pd.to_datetime(x, format = date_format))
        return df
    
class FeatureEngineering:
    def __init__(self):
        pass
    
class DataFactory:
    def get_formatter(self, format):
        if format == 'Cleaning':
            return DataCleaning()
        elif format == 'Features':
            return FeatureEngineering()
        else:
            ValueError(format)
        












