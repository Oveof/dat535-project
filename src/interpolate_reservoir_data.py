import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def generate_synthetic_power_prices(input_file, start_year=1995):
    df = pd.read_csv(input_file)
    
    df['Dato/klokkeslett'] = pd.to_datetime(df['Dato/klokkeslett'], format='%Y-%m-%d Kl. %H-%M')
    
    original_start = df['Dato/klokkeslett'].min()
    
    synthetic_start = datetime(start_year, 1, 1)
    synthetic_end = original_start - timedelta(hours=1)
    
    date_range = pd.date_range(start=synthetic_start, end=synthetic_end, freq='H')
    
    synthetic_df = pd.DataFrame(index=date_range, columns=df.columns[1:])
    synthetic_df.index.name = 'timestamp'
    
    mean_prices = df[df.columns[1:]].mean()
    std_prices = df[df.columns[1:]].std()
    
    for column in synthetic_df.columns:
        base_prices = np.random.normal(mean_prices[column], std_prices[column], len(synthetic_df))
        
        years = (synthetic_df.index.year - start_year) / (original_start.year - start_year)
        yearly_trend = years * mean_prices[column] * 0.5  # 50% increase over the period
        
        seasonal = np.sin(2 * np.pi * synthetic_df.index.dayofyear / 365) * mean_prices[column] * 0.2
        
        daily = np.sin(2 * np.pi * synthetic_df.index.hour / 24) * mean_prices[column] * 0.1
        
        synthetic_df[column] = base_prices + yearly_trend + seasonal + daily
        
        synthetic_df[column] = synthetic_df[column].clip(lower=0)
        
        synthetic_df[column] = synthetic_df[column].round(5)
    
    synthetic_df = synthetic_df.reset_index()
    
    df = df.rename(columns={'Dato/klokkeslett': 'timestamp'})
    
    combined_df = pd.concat([synthetic_df, df], ignore_index=True)
    
    for region in ['NO1', 'NO2', 'NO3', 'NO4', 'NO5']:
        region_df = combined_df[['timestamp', region]].copy()
        region_df['region'] = region
        region_df = region_df.rename(columns={region: 'price'})
        
        output_file = f'power_prices_{region.lower()}.csv'
        region_df.to_csv(output_file, index=False)
        print(f"Saved {region} data to {output_file}")

    return combined_df

if __name__ == "__main__":
    df = generate_synthetic_power_prices('spotpriser.csv', start_year=1995)
