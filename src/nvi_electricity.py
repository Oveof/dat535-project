import requests
import json
import csv

url = "https://biapi.nve.no/nettleiestatistikk/api/Nettleie/HistorikkHusholdningFritid?FraDato=2005-01-01&TilDato=2024-10-30&AarligTotalForbrukHusholdning=20000&EffektHusholdning=4&AarligTotalForbrukFritidsbolig=4000&EffektFritidsbolig=1"

dataset_location = "./datasets/"
headers = {"Content-Type": "application/json"}

response = requests.get(url, headers=headers)
data = response.json()

with open(dataset_location + "nvi_electricity.json", "w") as f:
    json.dump(data, f)

 
 
 
csv_file = open('data_file.csv', 'w')
 
# create the csv writer object
csv_writer = csv.writer(csv_file)
 
# Counter variable used for writing 
# headers to the CSV file
count = 0
 
for emp in data:
    if count == 0:
 
        # Writing headers of CSV file
        header = emp.keys()
        csv_writer.writerow(header)
        count += 1
 
    # Writing data of CSV file
    csv_writer.writerow(emp.values())
 
csv_file.close()
