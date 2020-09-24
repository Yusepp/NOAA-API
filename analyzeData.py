import pandas as pd
import numpy as np
import ujson
import sys

def check_data(file):
    # Read csv into DataFrame and replace * to NaN
    df = pd.read_csv(file)
    df = df.where(df != '*',np.nan)
    # Select rows and columns with NaN
    sparse_columns = df.columns[df.isna().any()].tolist()
    sparse_rows = dict(df.isna().any(axis=1))
    sparse_rows = [row for row in sparse_rows if sparse_rows[row] == True]
    data = [sparse_columns,sparse_rows]
    print(ujson.dumps(data))

check_data(sys.argv[1])