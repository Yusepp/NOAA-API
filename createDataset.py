# Import libraries
import sys
import pandas as pd
import numpy as np
from os import listdir
from os.path import isfile, join
from copy import deepcopy


def create_dataset():
    u = sys.argv[2]
    v = sys.argv[3]
    w = sys.argv[4]
    
    df_u = pd.read_csv("fix/2009/1.csv")
    df_v = pd.read_csv("fix/2009/1.csv")
    df_w = pd.read_csv("fix/2009/1.csv")
    
    numerical_columns = df_u.select_dtypes('number').columns

create_dataset()