import configparser
import csv

import requests


parser = configparser.ConfigParser()
parser.read("pipeline.conf")
host = parser.get("api_config", "host")
port = parser.get("api_config", "port")

API_URL = f"http://{host}:{port}"
DATA_FOLDER = "data"

### Events
data_events = "events"
date_events = "2021-02-10"
response = requests.get(f"{API_URL}/{data_events}/?created_at={date_events}")
data = response.json()
with open(f"{DATA_FOLDER}/events.csv", "w") as f:
    writer = csv.writer(f)
    header = data[0].keys()
    writer.writerow(header)

    for each in data:
        writer.writerow(each.values())

### Users
data_users = "users"
date_users = "2020-10-23"
response = requests.get(f"{API_URL}/{data_users}/?created_at={date_users}")
data = response.json()
with open(f"{DATA_FOLDER}/users.csv", "w") as f:
    writer = csv.writer(f)
    header = data[0].keys()
    writer.writerow(header)

    for each in data:
        writer.writerow(each.values())
# ลองดึงข้อมูลจาก API เส้น users และเขียนลงไฟล์ CSV
# YOUR CODE HERE

### Orders
data_orders = "orders"
date_orders = "2021-02-10"
response = requests.get(f"{API_URL}/{data_orders}/?created_at={date_orders}")
data = response.json()
with open(f"{DATA_FOLDER}/orders.csv", "w") as f:
    writer = csv.writer(f)
    header = data[0].keys()
    writer.writerow(header)

    for each in data:
        writer.writerow(each.values())
# ลองดึงข้อมูลจาก API เส้น orders และเขียนลงไฟล์ CSV
# YOUR CODE HERE