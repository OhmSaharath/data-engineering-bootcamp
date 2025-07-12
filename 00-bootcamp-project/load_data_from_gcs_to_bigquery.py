import json

from google.cloud import bigquery
from google.oauth2 import service_account

BUSINESS_DOMAIN = "greenery"
project_id = "smooth-ripple-463708-g8"
location = "asia-southeast1"
bucket_name = "deb-bootcamp-031"
data_non_partition = ['addresses', 'order_items', 'products', 'promos', 'orders_with_size']
data_fact = ['orders_with_size']
data_partition = ['users', 'events', 'orders']
dt = {
     "a" : "date=2021-02-10",
     "b" : "date=2020-10-23"
    }
schemas = {
    "addresses": [
            bigquery.SchemaField("address_id", bigquery.SqlTypeNames.STRING),
            bigquery.SchemaField("address", bigquery.SqlTypeNames.STRING),
            bigquery.SchemaField("zipcode", bigquery.SqlTypeNames.STRING),
            bigquery.SchemaField("state", bigquery.SqlTypeNames.STRING),
            bigquery.SchemaField("country", bigquery.SqlTypeNames.STRING),
            ],
    "events" : [
            bigquery.SchemaField("event_id", bigquery.SqlTypeNames.STRING),
            bigquery.SchemaField("session_id", bigquery.SqlTypeNames.STRING),
            bigquery.SchemaField("page_url", bigquery.SqlTypeNames.STRING),
            bigquery.SchemaField("created_at", bigquery.SqlTypeNames.TIMESTAMP),
            bigquery.SchemaField("event_type", bigquery.SqlTypeNames.STRING),
            bigquery.SchemaField("user", bigquery.SqlTypeNames.STRING),
            bigquery.SchemaField("order", bigquery.SqlTypeNames.STRING),
            bigquery.SchemaField("product", bigquery.SqlTypeNames.STRING),
            ],
    "order_items": [
            bigquery.SchemaField("order_id", bigquery.SqlTypeNames.STRING),
            bigquery.SchemaField("product_id", bigquery.SqlTypeNames.STRING),
            bigquery.SchemaField("quantity", bigquery.SqlTypeNames.INTEGER),
            ],
    "orders": [
            bigquery.SchemaField("order_id", bigquery.SqlTypeNames.STRING),
            bigquery.SchemaField("created_at", bigquery.SqlTypeNames.TIMESTAMP),
            bigquery.SchemaField("order_cost", bigquery.SqlTypeNames.FLOAT),
            bigquery.SchemaField("shipping_cost", bigquery.SqlTypeNames.FLOAT),
            bigquery.SchemaField("order_total", bigquery.SqlTypeNames.FLOAT),
            bigquery.SchemaField("tracking_id", bigquery.SqlTypeNames.STRING),
            bigquery.SchemaField("shipping_service", bigquery.SqlTypeNames.STRING),
            bigquery.SchemaField("estimated_delivery_at", bigquery.SqlTypeNames.TIMESTAMP),
            bigquery.SchemaField("delivered_at", bigquery.SqlTypeNames.TIMESTAMP),
            bigquery.SchemaField("status", bigquery.SqlTypeNames.STRING),
            bigquery.SchemaField("user", bigquery.SqlTypeNames.STRING),
            bigquery.SchemaField("promo", bigquery.SqlTypeNames.STRING),
            bigquery.SchemaField("address", bigquery.SqlTypeNames.STRING),
            ],
    "products": [
            bigquery.SchemaField("product_id", bigquery.SqlTypeNames.STRING),
            bigquery.SchemaField("name", bigquery.SqlTypeNames.STRING),
            bigquery.SchemaField("price", bigquery.SqlTypeNames.FLOAT), # DoubleType maps to FLOAT64 in BigQuery
            bigquery.SchemaField("inventory", bigquery.SqlTypeNames.INTEGER),
            ],
    "promos": [
            bigquery.SchemaField("promo_id", bigquery.SqlTypeNames.STRING),
            bigquery.SchemaField("discount", bigquery.SqlTypeNames.INTEGER),
            bigquery.SchemaField("status", bigquery.SqlTypeNames.STRING),
            ],
    "users": [
            bigquery.SchemaField("user_id", bigquery.SqlTypeNames.STRING),
            bigquery.SchemaField("first_name", bigquery.SqlTypeNames.STRING),
            bigquery.SchemaField("last_name", bigquery.SqlTypeNames.STRING),
            bigquery.SchemaField("email", bigquery.SqlTypeNames.STRING),
            bigquery.SchemaField("phone_number", bigquery.SqlTypeNames.STRING),
            bigquery.SchemaField("created_at", bigquery.SqlTypeNames.TIMESTAMP),
            bigquery.SchemaField("updated_at", bigquery.SqlTypeNames.TIMESTAMP),
            bigquery.SchemaField("address", bigquery.SqlTypeNames.STRING),
            ]
}

# Prepare and Load Credentials to Connect to GCP Services
keyfile_bigquery = "deb-loading-data-to-bq.json"
service_account_info_bigquery = json.load(open(keyfile_bigquery))
credentials_bigquery = service_account.Credentials.from_service_account_info(
    service_account_info_bigquery
)

# Load data from GCS to BigQuery
bigquery_client = bigquery.Client(
    project=project_id,
    credentials=credentials_bigquery,
    location=location,
)

for table_non_par in data_non_partition :
    source_data_in_gcs = f"gs://{bucket_name}/cleaned/{BUSINESS_DOMAIN}/{table_non_par}/*.parquet"
    table_id = f"{project_id}.deb_bootcamp.{table_non_par}"
    if table_non_par in data_fact :
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.PARQUET,
            autodetect=True,
        )
    else :
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.PARQUET,
            # autodetect=True,
            schema=schemas[table_non_par],
        )
    job = bigquery_client.load_table_from_uri(
        source_data_in_gcs,
        table_id,
        job_config=job_config,
        location=location,
    )
    job.result()

    table = bigquery_client.get_table(table_id)
    print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

for table_par in data_partition :
    if table_par == "users" :
        source_data_in_gcs = f"gs://{bucket_name}/cleaned/{BUSINESS_DOMAIN}/{table_par}/{dt['b']}/*.parquet"
        partition_0 = dt['b'].replace("-", "")
        partition = partition_0.replace("date=", "")
    else :
        source_data_in_gcs = f"gs://{bucket_name}/cleaned/{BUSINESS_DOMAIN}/{table_par}/{dt['a']}/*.parquet"
        partition_0 = dt['a'].replace("-", "")
        partition = partition_0.replace("date=", "")
        
    table_id = f"{project_id}.deb_bootcamp.{table_par}${partition}"
    job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    source_format=bigquery.SourceFormat.PARQUET,
    # autodetect=True,
    schema=schemas[table_par],
    time_partitioning=bigquery.TimePartitioning(
    type_=bigquery.TimePartitioningType.DAY,
    field="created_at",
    ),
    )
    job = bigquery_client.load_table_from_uri(
        source_data_in_gcs,
        table_id,
        job_config=job_config,
        location=location,
    )
    job.result()

    table = bigquery_client.get_table(table_id)
    print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")