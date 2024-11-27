import requests
import json

url = "https://biapi.nve.no/magasinstatistikk/api/Magasinstatistikk/HentOffentligData"

dataset_location = "./datasets/"
headers = {"Content-Type": "application/json"}

response = requests.get(url, headers=headers)
data = response.json()

with open(dataset_location + "water_magazine.json", "w") as f:
    json.dump(data, f)
