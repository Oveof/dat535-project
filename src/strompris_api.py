import requests
import json
import time
from datetime import date, timedelta

dataset_location = "./datasets/"
headers = {"Content-Type": "application/json"}

def daterange(start_date: date, end_date: date):
    days = int((end_date - start_date).days)
    for n in range(days):
        yield start_date + timedelta(n)

from_year = 2023
to_year = 2023
total_years = to_year - from_year

start_date = date(from_year, 1, 1)
end_date = date(to_year, 12, 31)
date_range = daterange(start_date, end_date)

data = []
for i, single_date in enumerate(date_range):
    print(f"{i / (365 * total_years) * 100}%")
    year = single_date.year
    month = single_date.month if single_date.month >= 10 else "0" + str(single_date.month)
    day = single_date.day if single_date.day >= 10 else "0" + str(single_date.day)
    url = f"https://www.hvakosterstrommen.no/api/v1/prices/{year}/{month}-{day}_NO4.json" 
    response = requests.get(url, headers=headers)
    data.append(response.json())
    time.sleep(1)



with open(dataset_location + "strompriser.json", "w") as f:
    json.dump(data, f)
