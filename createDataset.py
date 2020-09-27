# Import libraries
import pandas as pd
import numpy as np
from os import listdir
from os.path import isfile, join
from copy import deepcopy

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

def load_fix_csv():
    # Check all csv
    files = [f for f in listdir('./fix') if isfile(join("./fix", f))]

    # Load main csv and fix it
    df_base = pd.read_csv('tmp.csv')
    df_base = df_base.where(df_base != '*',np.nan)

    # Load csv to merge in a list and fix them
    df_to_merge = [pd.read_csv('./fix/'+file) for file in files]
    df_to_merge = [df.where(df != '*',np.nan) for df in df_to_merge]

    for df in df_to_merge:
        del df['NAME']

    # Transform to real datetimes all data
    df_to_merge = [format_time(df) for df in df_to_merge]
    df_base = format_time(df_base)

    # Sort them
    df_to_merge = [df.sort_values(by="HR_TIME") for df in df_to_merge]
    df_base = df_base.sort_values(by="HR_TIME")
    
    # Change datatypes
    df_to_merge = [change_dtype(df) for df in df_to_merge]
    df_base = change_dtype(df_base)
    
    return df_base,df_to_merge

def fill_base(df_base,df_to_merge):

    for df in df_to_merge:
        df_base = df_base.combine_first(df)
    
    return df_base.reindex(columns=["HR_TIME","LAT","LON","TEMP","MIN","MAX","DIR","PCP01"])


def find_diff_dates(df_base):
    diff_dates = []
    
    for date in df_base['HR_TIME']:
        if date.date() not in diff_dates:
            diff_dates.append(date)
            
    return diff_dates

def create_masks(df_base, diff_dates):
    masks = []
    for date in diff_dates:
        masks.append(df_base['HR_TIME'].map(lambda x: x.date()) == date)
    return masks

def add_min_max(filtered_dfs):
    dfs = []
    for df in filtered_dfs:
        df['MIN'] = df['TEMP'].min()
        df['MAX'] = df['TEMP'].max()
        dfs.append(df)
    return dfs
        

def fill_min_max(df_base):
    diff_dates = find_diff_dates(df_base)
    masks = create_masks(df_base,diff_dates)
    filtered_dfs = [df_base[mask] for mask in masks]
    filtereds_min_max = add_min_max(filtered_dfs) 
    return pd.concat(filtereds_min_max)


def fill_missing(df_base,df_to_merge):
    df_base = fill_base(df_base,df_to_merge)
    df_base = fill_min_max(df_base)
    return df_base

def create_dataset():
    df_base, df_to_merge = load_fix_csv()
    df_base = fill_missing(df_base,df_to_merge)
    df_base.to_csv('./dataset.csv', index = False, header=True, sep=';')

create_dataset()