import csv
import configparser

import psycopg2


parser = configparser.ConfigParser()
parser.read("pipeline.conf")
dbname = parser.get("postgres_config", "database")
user = parser.get("postgres_config", "username")
password = parser.get("postgres_config", "password")
host = parser.get("postgres_config", "host")
port = parser.get("postgres_config", "port")

conn_str = f"dbname={dbname} user={user} password={password} host={host} port={port}"
conn = psycopg2.connect(conn_str)
cursor = conn.cursor()

DATA_FOLDER = "data"

table_addresses = "addresses"
header_addresses = ["address_id", "address", "zipcode", "state", "country"]
with open(f"{DATA_FOLDER}/addresses.csv", "w") as f:
    writer = csv.writer(f)
    writer.writerow(header_addresses)

    query = f"select * from {table_addresses}"
    cursor.execute(query)

    results = cursor.fetchall()
    for each in results:
        writer.writerow(each)

table_order_items = "order_items"
header_order_items = ["order_id", "product_id", "quantity"]
with open(f"{DATA_FOLDER}/order_items.csv", "w") as f:
    writer = csv.writer(f)
    writer.writerow(header_order_items)

    query = f"select * from {table_order_items}"
    cursor.execute(query)

    results = cursor.fetchall()
    for each in results:
        writer.writerow(each)
# ลองดึงข้อมูลจากตาราง order_items และเขียนลงไฟล์ CSV
# YOUR CODE HERE