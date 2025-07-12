from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType, DoubleType, TimestampType
from pyspark.sql.functions import col, to_date

# --- Configuration ---
KEYFILE_PATH = "/opt/spark/pyspark/deb-loading-data-to-gcs.json"

# Define lists of tables based on their partitioning strategy
data_non_partition = ['addresses', 'order_items', 'products', 'promos']
data_partition = ['users', 'events', 'orders']

# Combine all source tables into one list for processing
all_source_tables = data_non_partition + data_partition

# Define the dates needed for partitioned source files
# This replaces the hardcoded dates in the old path_config
source_dates = {
     "events": "2021-02-10",
     "orders": "2021-02-10",
     "users" : "2020-10-23"
}

# Base GCS paths
GCS_RAW_BUCKET = "gs://deb-bootcamp-031/raw/greenery"
GCS_CLEANED_BUCKET = "gs://deb-bootcamp-031/cleaned/greenery"


# --- Spark Session Initialization ---
spark = SparkSession.builder.appName("transform-refactored") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "5g") \
    .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("google.cloud.auth.service.account.enable", "true") \
    .config("google.cloud.auth.service.account.json.keyfile", KEYFILE_PATH) \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
    .config("spark.hadoop.fs.gs.outputstream.upload.chunk.size", "1048576") \
    .getOrCreate()


# --- Schemas Definition (remains the same) ---
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


# --- Main Processing Loop for all source tables ---
for table_name in all_source_tables:
    print(f"Processing table: {table_name}...")

    # Dynamically generate input and output paths
    output_path = f"{GCS_CLEANED_BUCKET}/{table_name}/"
    
    if table_name in data_partition:
        # For partitioned data, add the date to the input path
        date_folder = source_dates[table_name]
        input_path = f"{GCS_RAW_BUCKET}/{table_name}/{date_folder}/{table_name}.csv"
    else:
        # For non-partitioned data, the path is simpler
        input_path = f"{GCS_RAW_BUCKET}/{table_name}/{table_name}.csv"

    # Read data from GCS
    df = spark.read \
        .option("header", True) \
        .schema(schemas[table_name]) \
        .csv(input_path)

    print(f"Schema and data for {table_name}:")
    df.printSchema()
    df.show(5)

    # For this example, we just select all columns. 
    # You could add more transformations here.
    result = df.select("*")

    print(f"Writing {table_name} to {output_path}...")
    if table_name in data_partition:
        # Add a 'date' column from 'created_at' for partitioning
        result_with_date = result.withColumn("date", to_date(col("created_at")))
        result_with_date.write.partitionBy("date").mode("overwrite").parquet(output_path)
    else:
        # Write non-partitioned data directly
        result.write.mode("overwrite").parquet(output_path)

    print(f"Finished processing {table_name}.\n{'-'*40}")


# --- Special Processing for 'orders_with_size' ---
print("Processing special table: orders_with_size...")

# Define paths for the join operation
orders_input_path = f"{GCS_RAW_BUCKET}/orders/{source_dates['orders']}/orders.csv"
order_items_input_path = f"{GCS_RAW_BUCKET}/order_items/order_items.csv"
orders_with_size_output_path = f"{GCS_CLEANED_BUCKET}/orders_with_size/"

# Read the two tables needed for the join
df_orders = spark.read \
    .option("header", True) \
    .schema(schemas['orders']) \
    .csv(orders_input_path)

df_order_items = spark.read \
    .option("header", True) \
    .schema(schemas['order_items']) \
    .csv(order_items_input_path)

# Create temp views to use Spark SQL
df_orders.createOrReplaceTempView("orders")
df_order_items.createOrReplaceTempView("order_items")

# Perform the aggregation query
orders_with_size_result = spark.sql("""
    WITH orders_with_size AS (
        SELECT
            o.*,
            oi.quantity,
            CASE
                WHEN oi.quantity <= 2 THEN 'S'
                WHEN oi.quantity > 2 AND oi.quantity <= 3 THEN 'M'
                ELSE 'L'
            END AS size
        FROM orders AS o
        JOIN order_items AS oi ON o.order_id = oi.order_id
    )
    SELECT
        size,
        COUNT(1) AS order_count
    FROM orders_with_size
    GROUP BY size
""")

print("Result for orders_with_size:")
orders_with_size_result.show()

print(f"Writing orders_with_size to {orders_with_size_output_path}...")
orders_with_size_result.write.mode("overwrite").parquet(orders_with_size_output_path)
print(f"Finished processing orders_with_size.\n{'-'*40}")

spark.stop()
