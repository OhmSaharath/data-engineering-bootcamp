from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType, DoubleType, TimestampType
from pyspark.sql.functions import col, to_date


KEYFILE_PATH = "/opt/spark/pyspark/deb-loading-data-to-gcs.json"
data_non_partition = ['addresses', 'order_items', 'products', 'promos']
data_partition = ['users', 'events', 'orders']

spark = SparkSession.builder.appName("transform") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "5g") \
    .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("google.cloud.auth.service.account.enable", "true") \
    .config("google.cloud.auth.service.account.json.keyfile", KEYFILE_PATH) \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
    .config("spark.hadoop.fs.gs.outputstream.upload.chunk.size", "1048576") \
    .getOrCreate()

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


dt = {
     "a" : "2021-02-10",
     "b" : "2020-10-23"
    }
path_config = {
    "addresses": {
        "input": "gs://deb-bootcamp-031/raw/greenery/addresses/addresses.csv",
        "output": "gs://deb-bootcamp-031/cleaned/greenery/addresses/"
    },
    "events": {
        "input": f"gs://deb-bootcamp-031/raw/greenery/events/{dt['a']}/events.csv",
        "output": f"gs://deb-bootcamp-031/cleaned/greenery/events/"
    },
    "order_items": {
        "input": "gs://deb-bootcamp-031/raw/greenery/order_items/order_items.csv",
        "output": "gs://deb-bootcamp-031/cleaned/greenery/order_items/"
    },
    "orders": {
        "input": f"gs://deb-bootcamp-031/raw/greenery/orders/{dt['a']}/orders.csv",
        "output": f"gs://deb-bootcamp-031/cleaned/greenery/orders/"
    },
    "products": {
        "input": "gs://deb-bootcamp-031/raw/greenery/products/products.csv",
        "output": "gs://deb-bootcamp-031/cleaned/greenery/products/"
    },
    "promos": {
        "input": "gs://deb-bootcamp-031/raw/greenery/promos/promos.csv",
        "output": "gs://deb-bootcamp-031/cleaned/greenery/promos/"
    },
    "users": {
        "input": f"gs://deb-bootcamp-031/raw/greenery/users/{dt['b']}/users.csv",
        "output": f"gs://deb-bootcamp-031/cleaned/greenery/users/"
    },
    "orders_with_size" : {
        "input_items": "gs://deb-bootcamp-031/raw/greenery/order_items/order_items.csv",
        "input_orders": f"gs://deb-bootcamp-031/raw/greenery/orders/{dt['a']}/orders.csv",
        "output": f"gs://deb-bootcamp-031/cleaned/greenery/orders_with_size/"
    }
}

for table_name, config in path_config.items():
    print(f"Processing table: {table_name}...")
    if table_name == "orders_with_size" :
        df_orders = spark.read \
            .option("header", True) \
            .schema(schemas['orders']) \
            .csv(config['input_orders'])
        
        df_order_items = spark.read \
            .option("header", True) \
            .schema(schemas['order_items']) \
            .csv(config['input_items']) \

        df_orders.createOrReplaceTempView("orders")
        df_order_items.createOrReplaceTempView("order_items")
        result = spark.sql("""
            with

        orders_with_size as (

            select
                *
                , case
                    when quantity <= 2 then 'S'
                    when quantity > 2 and quantity <= 3 then 'M'
                    else 'L'
                end as size
            from orders as o
            join order_items as oi
            on o.order_id = oi.order_id

        )

        select
            size
            , count(1) as order_count
        from orders_with_size
        group by size
            """)
        result.show
        result.write.mode("overwrite").parquet(config['output'])
    else :
        df = spark.read \
            .option("header", True) \
            .schema(schemas[table_name]) \
            .csv(config["input"])

        print(f"Schema and data for {table_name}:")
        df.printSchema()
        df.show(5)

        df.createOrReplaceTempView(table_name)
        result = spark.sql(f"""
        select
            *

        from {table_name}
        """)
    print(f"Writing {table_name} to {config['output']}...")
    if table_name in data_partition : 
        result_with_date = result.withColumn("date", to_date(col("created_at")))
        result_with_date.write.partitionBy("date").mode("overwrite").parquet(config["output"])
    else :
        result.write.mode("overwrite").parquet(config["output"])

    print(f"Finished processing {table_name}.\n{'-'*40}")