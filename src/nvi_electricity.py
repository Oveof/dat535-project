import requests
import json

url = "https://biapi.nve.no/nettleiestatistikk/api/Nettleie/HistorikkHusholdningFritid?FraDato=2005-01-01&TilDato=2024-10-30&AarligTotalForbrukHusholdning=20000&EffektHusholdning=4&AarligTotalForbrukFritidsbolig=4000&EffektFritidsbolig=1"

dataset_location = "./datasets/"
headers = {"Content-Type": "application/json"}

response = requests.get(url, headers=headers)
data = response.json()

with open(dataset_location + "nvi_electricity.json", "w") as f:
    json.dump(data, f)
