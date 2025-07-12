import sys
import os
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType, DoubleType, TimestampType
from pyspark.sql import SparkSession


BUSINESS_DOMAIN = "greenery"
BUCKET_NAME = "deb-bootcamp-031"
KEYFILE_PATH = "/opt/airflow/config/deb-loading-data-to-gcs.json"
data_non_partition = ['addresses', 'order_items', 'products', 'promos']
data_partition = ['users', 'events', 'orders']
execution_date = os.getenv("EXECUTION_DATE")

schemas = {
    "addresses": StructType([
        StructField("address_id", StringType()),
        StructField("address", StringType()),
        StructField("zipcode", StringType()),
        StructField("state", StringType()),
        StructField("country", StringType()),
    ]),
    "events": StructType([
        StructField("event_id", StringType()),
        StructField("session_id", StringType()),
        StructField("page_url", StringType()),
        StructField("created_at", TimestampType()),
        StructField("event_type", StringType()),
        StructField("user", StringType()),
        StructField("order", StringType()),
        StructField("product", StringType()),
    ]),
    "order_items": StructType([
        StructField("order_id", StringType()),
        StructField("product_id", StringType()),
        StructField("quantity", IntegerType()),
    ]),
    "orders": StructType([
        StructField("order_id", StringType()),
        StructField("created_at", TimestampType()),
        StructField("order_cost", FloatType()),
        StructField("shipping_cost", FloatType()),
        StructField("order_total", FloatType()),
        StructField("tracking_id", StringType()),
        StructField("shipping_service", StringType()),
        StructField("estimated_delivery_at", TimestampType()),
        StructField("delivered_at", TimestampType()),
        StructField("status", StringType()),
        StructField("user", StringType()),
        StructField("promo", StringType()),
        StructField("address", StringType()),    
    ]),
    "products": StructType([
        StructField("product_id", StringType()),
        StructField("name", StringType()),
        StructField("price", DoubleType()),
        StructField("inventory", IntegerType()),
    ]),
    "promos": StructType([
        StructField("promo_id", StringType()),
        StructField("discount", IntegerType()),
        StructField("status", StringType()),
    ]),
    "users": StructType([
        StructField("user_id", StringType()),
        StructField("first_name", StringType()),
        StructField("last_name", StringType()),
        StructField("email", StringType()),
        StructField("phone_number", StringType()),
        StructField("created_at", TimestampType()),
        StructField("updated_at", TimestampType()),
        StructField("address_id", StringType()),
    ])
}

def transform_data(data:str) :

    spark = SparkSession.builder.appName(data) \
        .config("spark.memory.offHeap.enabled", "true") \
        .config("spark.memory.offHeap.size", "5g") \
        .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("google.cloud.auth.service.account.enable", "true") \
        .config("google.cloud.auth.service.account.json.keyfile", KEYFILE_PATH) \
        .getOrCreate()

    INPUT_PATH = f"gs://{BUCKET_NAME}/raw/{BUSINESS_DOMAIN}/{data}/{data}.csv"
    OUTPUT_PATH = f"gs://{BUCKET_NAME}/cleaned/{BUSINESS_DOMAIN}/{data}"

    df = spark.read.option("header", True).schema(schemas[data]).csv(INPUT_PATH)
    df.show()

    df.createOrReplaceTempView(f"{data}")
    result = spark.sql(f"""
        select
            *

        from {data}
    """)

    result.write.mode("overwrite").parquet(OUTPUT_PATH)

def transform_data_par(data:str) :

    spark = SparkSession.builder.appName(data) \
        .config("spark.memory.offHeap.enabled", "true") \
        .config("spark.memory.offHeap.size", "5g") \
        .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("google.cloud.auth.service.account.enable", "true") \
        .config("google.cloud.auth.service.account.json.keyfile", KEYFILE_PATH) \
        .getOrCreate()

    INPUT_PATH = f"gs://{BUCKET_NAME}/raw/{BUSINESS_DOMAIN}/{data}/{execution_date}/{data}.csv"
    OUTPUT_PATH = f"gs://{BUCKET_NAME}/cleaned/{BUSINESS_DOMAIN}/{data}/{execution_date}"

    df = spark.read.option("header", True).schema(schemas[data]).csv(INPUT_PATH)
    df.show()

    df.createOrReplaceTempView(f"{data}")
    result = spark.sql(f"""
        select
            *

        from {data}
    """)

    result.write.mode("overwrite").parquet(OUTPUT_PATH)

if __name__ == "__main__" :
    if len(sys.argv) < 2 :
        print("Error mission <data>")
        sys.exit(-1)

    data_process = sys.argv[1]

    if data_process in data_partition :
        transform_data_par(data_process)
    else :
        transform_data(data_process)