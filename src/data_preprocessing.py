from pyspark import SparkContext
import csv
from io import StringIO
from datetime import datetime

# Initialize SparkContext
sc = SparkContext("local", "CSV Data Preprocessing")

# Load the CSV file
file_path = "/mnt/data/data_file.csv"
data = sc.textFile(file_path)

# Define a function to parse each line as CSV
def parse_csv(line):
    # Use Python's CSV reader to handle parsing
    reader = csv.reader(StringIO(line))
    return next(reader)

# Parse each line in the CSV
parsed_data = data.map(parse_csv)

# Filter out header and rows with missing critical fields (e.g., if column 1 and 2 are critical)
header = parsed_data.first()  # Assumes the first row is the header
data_no_header = parsed_data.filter(lambda row: row != header)

# Deduplicate records using a unique key, for example, combining values of certain fields
# Let's assume fields at indices 1 and 2 make a unique identifier for each record
unique_data = data_no_header.map(lambda row: ((row[1], row[2]), row)) \
                            .reduceByKey(lambda x, _: x) \
                            .map(lambda x: x[1])

# Standardize date formats (assuming date is in the third column)
def standardize_dates(row):
    date_index = 3  # Adjust according to the actual date column index in your data
    try:
        # Parse and format date to 'YYYY-MM-DD'
        row[date_index] = datetime.strptime(row[date_index], "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%d")
    except ValueError:
        pass  # Skip reformatting if date is invalid
    return row

standardized_data = unique_data.map(standardize_dates)

# Clean numerical fields (assuming fields at indices 4 and 5 are numeric)
def clean_numerical_fields(row):
    numeric_fields = [4, 5]  # Adjust field indices as necessary
    for index in numeric_fields:
        if row[index]:
            try:
                row[index] = float(row[index].replace(',', '').strip())
            except ValueError:
                row[index] = None  # Set to None or a default value if conversion fails
    return row

final_data = standardized_data.map(clean_numerical_fields)

# Collecting results (or further processing)
final_output = final_data.collect()

# Stop SparkContext
sc.stop()

# Result is stored in `final_output`
print(final_data)
