#!/usr/bin/env python3

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/..")


from libs.ppstdlib import list_of_files
import pandas as pd
from datetime import datetime as dt

if len (sys.argv) != 2 :
    print ("Usage: Directory of Data")
    sys.exit (1)

arguments = sys.argv[1:]


# setting up directory path
dirpath = '/var/prism/Central/raw/amfi/{}/'.format(arguments[0])

# Get all csv files to process
list_of_files = list_of_files(dirpath, "csv")

# Extract times from the files and make list of times and sort it
time_list = set()

for path in list_of_files:
    if path:
        if "_" in path:
            time = path.rsplit('.')[0].rsplit('_')[1]
            time_list.add(dt.strptime(time, "%H:%M:%S"))

time_list = sorted(time_list)


if list_of_files:
    date = list_of_files[0].rsplit('.')[0].rsplit('_')[0]

# Get earliest file and load into pandas Data Frame
time_s = dt.strftime(time_list[1], "%H:%M:%S")
file = "{}_{}.csv".format(date, time_s)
merged_df = pd.read_csv(dirpath + file)
merged_df = merged_df[['Scheme Name','Net Asset Value']]
print(merged_df.head())
merged_df = start_df = merged_df.rename(columns = {'Net Asset Value':'pri'})

# Rename the name of the column 'pri' with 'pri_<time_of_the_file>'
start_suffix = dt.strftime(time_list[1], "_%H:%M")
merged_df = merged_df.rename(columns = {'pri':'pri{}'.format(start_suffix)})

for time in time_list[2:]:
    time_s = dt.strftime(time, "%H:%M:%S")
    end_prefix = dt.strftime(time, "_%H:%M")
    file = "{}_{}.csv".format(date, time_s)
    frame = pd.read_csv(dirpath + file)

    frame = frame[['Scheme Name','Net Asset Value']]
    print(frame.head())
    frame = frame.rename(columns = {'Net Asset Value':'pri'})

    inter_df = pd.merge(start_df, frame, on='Scheme Name', how='inner', 
            suffixes=[start_suffix, end_prefix])

    merged_df = pd.merge(merged_df, inter_df[['Scheme Name','pri'+end_prefix]], on='Scheme Name', how='right')

    start_df = frame
    start_suffix = end_prefix

# print(merged_df.info())
# merged_df = merged_df.drop(['pri'],axis=1)
# merged_df = merged_df.loc[:, ~merged_df.columns.str.endswith('_y')]
print(merged_df.head())

start = dt.strftime(time_list[1], "%H:%M")
for time in time_list[2:]:
    end = dt.strftime(time, "%H:%M")
    print("Comparing prices consistency between {} and {}".format(start,end))
    print(merged_df.loc[merged_df['pri_'+start]!=merged_df['pri_'+end]].dropna())
    print("---------------------------------------------------------------------")
    start = end
