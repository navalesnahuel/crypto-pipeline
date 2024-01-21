import requests
import logging
import requests
from datetime import datetime, timezone
import pandas as pd
from io import StringIO
from airflow.hooks.base_hook import BaseHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import os

# Create a exception

class ResponseError(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(message)


# This retrieves historical price data for
# a list of asset IDs from the CoinCap API. It creates a dictionary to store
# the historical prices for each asset and then converts this dictionary into
# a pandas DataFrame.


def get_data(n):
    url = 'https://api.coincap.io/v2/assets'
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        ids = []

        for asset in data['data']:
            asset_rank = asset.get("rank")

            if asset_rank is not None and int(asset_rank) <= n:
                asset_id = asset.get("id")
                ids.append(asset_id)

                if len(ids) >= n:
                    break
            else:
                continue
    else:
        logging.error(f"Error fetching top assets: {response.text}")
        raise ResponseError(f"API request failed: {response.status_code}\n{response.text}")
    
# Part Two.
    
    asset_data_dict = {}

    for asset_id in ids:
        url = f'https://api.coincap.io/v2/assets/{asset_id}/history?interval=d1'
        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()
            time_prices = {}

            for entry in data['data']:
                time = datetime.fromtimestamp(int(entry.get("time")) // 1000, timezone.utc)
                price = round(float(entry.get("priceUsd")), 3)
                time_prices[time.strftime('%Y-%m-%d')] = price

            asset_data_dict[asset_id] = time_prices
        else:
            logging.error(f"Error fetching data for asset {asset_id}: {response.text}")
            raise ResponseError(f"API request failed: {response.status_code}\n{response.text}")

    df = pd.DataFrame.from_dict(asset_data_dict, orient='index')

    with StringIO() as csv_buffer:
        df.to_csv(csv_buffer, index_label='Asset')
        csv_data = csv_buffer.getvalue()
    return csv_data


# Upload the CSV to GCS function

def upload_to_gcs(**kwargs):
    ti = kwargs.get('ti')
    if ti is not None:
        output_data = ti.xcom_pull(task_ids='get_data_task')

        if not output_data:
            raise ValueError("Output data not found in XCom from get_data_task.")

        current_dag_directory = os.path.dirname(os.path.abspath(__file__))
        relative_save_path = 'raw-data/'
        csv_file_name = 'crypto_raw_{}.csv'.format(datetime.now().strftime('%Y%m%d%H'))
        local_csv_path = os.path.join(current_dag_directory, relative_save_path, csv_file_name)
        os.makedirs(os.path.join(current_dag_directory, relative_save_path), exist_ok=True)


        with open(local_csv_path, 'w') as csv_file:
            csv_file.write(output_data)

        gcs_conn_id = os.getenv('GOOGLE_CLOUD_STORAGE_CONN_ID', 'google_cloud_storage_default')
        gcs_hook = GCSHook(gcs_conn_id)

        bucket_name = 'data-bucket-crypto'
        destination_path = os.path.join(relative_save_path, csv_file_name)

        gcs_hook.upload(bucket_name=bucket_name,
                        object_name=destination_path,
                        filename=local_csv_path,
                        mime_type='text/csv') 
    else:
        raise ValueError("Output data not found in XCom from get_data_task.")
