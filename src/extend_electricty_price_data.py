import pandas as pd
import numpy as np
import os

def read_and_process_data(file_path):
    df = pd.read_csv(file_path)
    
    df['dato_Id'] = pd.to_datetime(df['dato_Id'])
    
    df = df.drop_duplicates(subset=['dato_Id', 'omrnr'])
    
    grouped = df.groupby('omrnr')
    
    interpolated_data = {}
    
    for omrnr, group_df in grouped:
        group_df = group_df.sort_values('dato_Id')
        
        start_date = group_df['dato_Id'].min()
        end_date = group_df['dato_Id'].max()
        
        hourly_df = pd.DataFrame(index=pd.date_range(start=start_date, 
                                                    end=end_date, 
                                                    freq='h'))
        
        numeric_columns = ['fyllingsgrad', 'fylling_TWh', 'fyllingsgrad_forrige_uke', 
                         'endring_fyllingsgrad']
        
        for col in numeric_columns:
            temp_df = pd.DataFrame(index=group_df['dato_Id'])
            temp_df[col] = group_df[col].values
            
            hourly_df[col] = temp_df.reindex(hourly_df.index)[col].interpolate(method='linear')
        
        non_numeric_columns = ['omrType', 'iso_aar', 'iso_uke', 'kapasitet_TWh']
        for col in non_numeric_columns:
            temp_df = pd.DataFrame(index=group_df['dato_Id'])
            temp_df[col] = group_df[col].values
            
            hourly_df[col] = temp_df.reindex(hourly_df.index)[col].ffill()
        
        hourly_df['omrnr'] = omrnr
        
        hourly_df['dato_Id'] = hourly_df.index
        
        interpolated_data[omrnr] = hourly_df
    
    return interpolated_data

    
if __name__ == "__main__":
    input_file = "water_magazine.csv"  # Replace with your file path
    interpolated_data = read_and_process_data(input_file)

    os.makedirs("./result", exist_ok=True)
    
    for omrnr, df in interpolated_data.items():
        filename = os.path.join("./result", f'reservoir_data_{omrnr}.csv')
        
        df.to_csv(filename, index=False)
