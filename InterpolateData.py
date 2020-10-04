import os
import sys
import pandas as pd
import numpy as np
from pandas.core.indexes import base

# Create real datetimes
def format_time(df):
    df['HR_TIME'] = pd.to_datetime(df['HR_TIME'], format="%Y%m%d%H")
    return df

def change_dtype(df):
    df['TEMP'] = df['TEMP'].astype(float)
    df['MIN'] = df['MIN'].astype(float)
    df['MAX'] = df['MAX'].astype(float)
    df['DIR'] = df['DIR'].astype(float)
    df['PCP01'] = df['PCP01'].astype(float)
    
    if 'LAT' in df.columns and 'LON' in df.columns:
        df['LAT'] = df['LAT'].astype(float)
        df['LON'] = df['LON'].astype(float)
    
    return df

def find_diff_dates(df):
    diff_dates = []
    
    for date in df['HR_TIME']:
        if date.date() not in diff_dates:
            diff_dates.append(date)
            
    return diff_dates

def create_masks(df, diff_dates):
    masks = []
    for date in diff_dates:
        masks.append(df['HR_TIME'].map(lambda x: x.date()) == date)
    return masks

def add_min_max(filtered_dfs):
    dfs = []
    for df in filtered_dfs:
        df['MIN'] = df['TEMP'].min()
        df['MAX'] = df['TEMP'].max()
        dfs.append(df)
    return dfs
        

def fill_min_max(df):
    diff_dates = find_diff_dates(df)
    masks = create_masks(df,diff_dates)
    filtered_dfs = [df[mask] for mask in masks]
    filtereds_min_max = add_min_max(filtered_dfs) 
    return pd.concat(filtereds_min_max)

def get_datasets(files,base):
    datasets = [pd.read_csv(base+"/"+file) for file in files]
    datasets = [df.where(df != '*',np.nan) for df in datasets]
    datasets = [format_time(df) for df in datasets]
    datasets = [change_dtype(df) for df in datasets]
    
    for df in datasets:
        del df['NAME']
    
    datasets = [fill_min_max(df) for df in datasets]
    
    return datasets

def save_datasets(datasets,files,base):
    for i in range(len(datasets)):
        datasets[i].to_csv(base+"/"+files[i])


def process_data(base):
    files = [path for path in os.listdir(base)]
    datasets = get_datasets(files,base)
    datasets = [df.interpolate() for df in datasets]
    save_datasets(datasets,files,base)

process_data(sys.argv[1])