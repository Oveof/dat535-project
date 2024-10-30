from pyspark import SparkContext
from datetime import datetime
import json

from pyspark import SparkContext
import json

# Initialize SparkContext
sc = SparkContext("local", "Data Preprocessing")

# Load JSON file
file_path = "./dataset/test.json"
data = sc.textFile(file_path).map(lambda line: json.loads(line))

# 1. Filter out records with missing or null critical values
cleaned_data = data.filter(lambda record: record['konsesjonar'] is not None and record['tariffdato'] is not None)

# 2. Remove duplicates by creating a unique key and using reduceByKey
# Assuming 'organisasjonsnr' and 'tariffdato' create a unique entry
unique_data = cleaned_data.map(lambda record: ((record['organisasjonsnr'], record['tariffdato']), record)) \
                          .reduceByKey(lambda x, _: x) \
                          .map(lambda x: x[1])

# 3. Standardize fields (e.g., ensure all dates are in 'YYYY-MM-DD' format)
from datetime import datetime

def standardize_dates(record):
    date_fields = ['tariffdato', 'periodeFraDato', 'periodeTilDato']
    for field in date_fields:
        if field in record:
            try:
                # Convert to desired date format
                record[field] = datetime.strptime(record[field], "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%d")
            except ValueError:
                pass  # If date is invalid, leave as-is or apply additional handling
    return record

standardized_data = unique_data.map(standardize_dates)

# 4. Clean numerical fields by removing special characters and converting to float
def clean_numerical_fields(record):
    numerical_fields = ['fastleddEks', 'energileddEks', 'omregnetAarEks']
    for field in numerical_fields:
        if field in record and isinstance(record[field], str):
            # Remove special characters and convert to float
            record[field] = float(record[field].replace(',', ''))
    return record

final_data = standardized_data.map(clean_numerical_fields)

# Collecting results (or further processing)
final_output = final_data.collect()

# Stop SparkContext
sc.stop()

# Result is stored in `final_output`
