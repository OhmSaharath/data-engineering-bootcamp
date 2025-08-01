import csv
import json
import logging

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils import timezone

from google.cloud import bigquery
from google.oauth2 import service_account


DAGS_FOLDER = "/opt/airflow/dags"
BUSINESS_DOMAIN = "networkrail"
DATA = "movements"
LOCATION = "asia-southeast1"
GCP_PROJECT_ID = "smooth-ripple-463708-g8"
GCS_BUCKET = "deb-bootcamp-031"
BIGQUERY_DATASET = "networkrail"
KEYFILE_FOR_GCS = f"{DAGS_FOLDER}/upload_to_gcs.json"
KEYFILE_FOR_GCS_TO_BIGQUERY = f"{DAGS_FOLDER}/upload_to_bq.json"


def _load_data_from_gcs_to_bigquery(data_interval_start, **context):
    ds = data_interval_start.to_date_string()

    service_account_info_bigquery = json.load(open(KEYFILE_FOR_GCS_TO_BIGQUERY))
    credentials_bigquery = service_account.Credentials.from_service_account_info(
        service_account_info_bigquery
    )

    bigquery_client = bigquery.Client(
        project=GCP_PROJECT_ID,
        credentials=credentials_bigquery,
        location=LOCATION,
    )
    partition = ds.replace("-", "")
    table_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{DATA}${partition}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.PARQUET,
        autodetect=True,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="actual_timestamp",
        ),
    )

    source_blob_name = f"{BUSINESS_DOMAIN}/processed/*.parquet"
    job = bigquery_client.load_table_from_uri(
        f"gs://{GCS_BUCKET}/{source_blob_name}",
        table_id,
        job_config=job_config,
        location=LOCATION,
    )

    job.result()

    table = bigquery_client.get_table(table_id)
    print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")


default_args = {
    "owner": "Skooldio",
    "start_date": timezone.datetime(2024, 8, 25),
}
with DAG(
    dag_id="networkrail_movements",
    default_args=default_args,
    schedule=None,  # Set the schedule here
    catchup=False,
    tags=["DEB", "Skooldio", "networkrail"],
    max_active_runs=3,
):

    # Start
    start = EmptyOperator(task_id="start")

    # Transform data in data lake using Spark
    transform_data = SparkSubmitOperator(
        task_id="transform_data",
        application="/opt/spark/pyspark/transform.py",
        conn_id="my_spark", 
    )

    # Load data from GCS to BigQuery
    load_data_from_gcs_to_bigquery = PythonOperator(
        task_id="load_data_from_gcs_to_bigquery",
        python_callable = _load_data_from_gcs_to_bigquery
    )
    # End
    end = EmptyOperator(task_id="end", trigger_rule="one_success")

    # Task dependencies
    start >> transform_data >> load_data_from_gcs_to_bigquery >> end