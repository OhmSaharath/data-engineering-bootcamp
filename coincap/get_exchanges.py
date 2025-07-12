import csv

import requests

API_HEADERS = {
    "accept": "application/json",
    "apiKey": "1bc55b20e5cdfc47535414fee60ddc185fbff476a2f6e43f8ef5ebe44401bb67"
}
# Read data from API
url = "https://rest.coincap.io/v3/exchanges"
response = requests.get(url, API_HEADERS)
data = response.json()["data"]

# Write data to CSV
with open("exchanges.csv", "w") as f:
    fieldnames = [
        "exchangeId",
        "name",
        "rank",
        "percentTotalVolume",
        "volumeUsd",
        "tradingPairs",
        "socket",
        "exchangeUrl",
        "updated",
    ]
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    for each in data:
        writer.writerow(each)