import json

from google.cloud import storage
from google.oauth2 import service_account


DATA_FOLDER = "data"
BUSINESS_DOMAIN = "greenery"
project_id = "smooth-ripple-463708-g8"
location = "asia-southeast1"
bucket_name = "deb-bootcamp-031"
data_non_partition = ['addresses', 'order_items', 'products', 'promos']
data_partition = ['users', 'events', 'orders']
dt = "2021-02-10"
dt_users = "2020-10-23"
# Prepare and Load Credentials to Connect to GCP Services
keyfile_gcs = "deb-loading-data-to-gcs.json"
service_account_info_gcs = json.load(open(keyfile_gcs))
credentials_gcs = service_account.Credentials.from_service_account_info(
    service_account_info_gcs
)

# Load data from Local to GCS
storage_client = storage.Client(
    project=project_id,
    credentials=credentials_gcs,
)
bucket = storage_client.bucket(bucket_name)

for data_ingest in data_non_partition :
    file_path = f"{DATA_FOLDER}/{data_ingest}.csv"
    destination_blob_name = f"raw/{BUSINESS_DOMAIN}/{data_ingest}/{data_ingest}.csv"
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_path)

for data_par_ingest in data_partition :
    if data_par_ingest == 'users' : 
        partition = dt.replace("-", "")
        file_path = f"{DATA_FOLDER}/{data_par_ingest}.csv"
        destination_blob_name = f"raw/{BUSINESS_DOMAIN}/{data_par_ingest}/{dt_users}/{data_par_ingest}.csv"
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(file_path)
    else : 
        partition = dt.replace("-", "")
        file_path = f"{DATA_FOLDER}/{data_par_ingest}.csv"
        destination_blob_name = f"raw/{BUSINESS_DOMAIN}/{data_par_ingest}/{dt}/{data_par_ingest}.csv"
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(file_path)   
# YOUR CODE HERE TO LOAD DATA TO GCS
