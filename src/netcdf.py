from netCDF4 import Dataset

# Open the NetCDF file
file_path = '../datasets/met_analysis_1_0km_nordic_latest.nc'
dataset = Dataset(file_path, mode='r')

# Explore file metadata
print(dataset)

# Access variables
variable_names = dataset.variables.keys()
print("Variables:", variable_names)

# Read specific data

# Close the file
dataset.close()

