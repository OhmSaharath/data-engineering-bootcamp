import csv
import json

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils import timezone

import requests
from google.cloud import bigquery, storage
from google.oauth2 import service_account


BUSINESS_DOMAIN = "greenery"
LOCATION = "asia-southeast1"
GCP_PROJECT_ID = "smooth-ripple-463708-g8"
DAGS_FOLDER = "/opt/airflow/dags"
CONFIG_FOLDER = "/opt/airflow/config"
PYSPARK_FOLDER = "/opt/spark/pyspark"
DATA_CSV_FOLDER = "/opt/airflow/data_csv"
DATA = "promos"

"""
# Connection

## Spark

Connection Id: my_spark
Connection Type: Spark
Host: spark://spark-master
Port: 7077
Deploy mode: client
Spark binary: spark-submit
"""

def _extract_data():
    url = f"http://34.87.139.82:8000/{DATA}/"
    response = requests.get(url)
    data = response.json()

    with open(f"{DATA_CSV_FOLDER}/{DATA}/{DATA}.csv", "w") as f:
        writer = csv.writer(f)
        header = [
            "promo_id",
            "discount",
            "status",
        ]
        writer.writerow(header)
        for each in data:
            data = [
                each["promo_id"],
                each["discount"],
                each["status"],

            ]
            writer.writerow(data)


def _load_data_to_gcs():
    keyfile_gcs = f"{CONFIG_FOLDER}/deb-loading-data-to-gcs.json"
    service_account_info_gcs = json.load(open(keyfile_gcs))
    credentials_gcs = service_account.Credentials.from_service_account_info(
        service_account_info_gcs
    )

    # Load data from Local to GCS
    bucket_name = "deb-bootcamp-031"
    storage_client = storage.Client(
        project=GCP_PROJECT_ID,
        credentials=credentials_gcs,
    )
    bucket = storage_client.bucket(bucket_name)

    file_path = f"{DATA_CSV_FOLDER}/{DATA}/{DATA}.csv"
    destination_blob_name = f"raw/{BUSINESS_DOMAIN}/{DATA}/{DATA}.csv"
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_path)


def _load_data_from_gcs_to_bigquery():
    keyfile_bigquery = f"{CONFIG_FOLDER}/deb-loading-data-to-bq.json"
    service_account_info_bigquery = json.load(open(keyfile_bigquery))
    credentials_bigquery = service_account.Credentials.from_service_account_info(
        service_account_info_bigquery
    )

    bigquery_client = bigquery.Client(
        project=GCP_PROJECT_ID,
        credentials=credentials_bigquery,
        location=LOCATION,
    )

    table_id = f"{GCP_PROJECT_ID}.deb_bootcamp.{DATA}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.PARQUET,
        schema=[
            bigquery.SchemaField("promo_id", bigquery.SqlTypeNames.STRING),
            bigquery.SchemaField("discount", bigquery.SqlTypeNames.INTEGER),
            bigquery.SchemaField("status", bigquery.SqlTypeNames.STRING),
        ],
    )

    bucket_name = "deb-bootcamp-031"
    source_blob_name = f"cleaned/{BUSINESS_DOMAIN}/{DATA}/*.parquet"
    job = bigquery_client.load_table_from_uri(
        f"gs://{bucket_name}/{source_blob_name}",
        table_id,
        job_config=job_config,
        location=LOCATION,
    )
    job.result()

    table = bigquery_client.get_table(table_id)
    print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")


default_args = {
    "owner": "airflow",
    "start_date": timezone.datetime(2021, 2, 9),
}
with DAG(
    dag_id=f"greenery_{DATA}_data_pipeline",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=["DEB", "Skooldio", "greenery"],
):

    # Extract data from Postgres, API, or SFTP
    extract_data = PythonOperator(
        task_id="extract_data",
        python_callable = _extract_data
    )

    # Load data to GCS
    load_data_to_gcs = PythonOperator(
        task_id="load_data_to_gcs",
        python_callable = _load_data_to_gcs
    )
    
    # Submit a Spark app to transform data
    transform_data = SparkSubmitOperator(
        task_id="transform_data",
        application=f"{PYSPARK_FOLDER}/transform_data.py",
        application_args = [f"{DATA}"],
        conn_id="my_spark", 
    )

    # Load data from GCS to BigQuery
    load_data_from_gcs_to_bigquery = PythonOperator(
        task_id="load_data_from_gcs_to_bigquery",
        python_callable = _load_data_from_gcs_to_bigquery
    )

    # Task dependencies
    extract_data >> load_data_to_gcs >> transform_data >> load_data_from_gcs_to_bigquery