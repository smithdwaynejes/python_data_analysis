#!/usr/bin/env python3
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/..")


from libs.ppstdlib import list_of_files
import pandas as pd
from datetime import datetime as dt

if len (sys.argv) < 2 :
    print ("Usage: Directories of Data")
    sys.exit (1)

arguments = sys.argv[1:]


pieces = []
date_dict = {}

dirpath = '/var/prism/Central/raw/amfi/'

for argument in arguments:
    dirpath_ = '/var/prism/Central/raw/amfi/{}/'.format(argument)

    # Get all csv files to process
    list_of_files = list_of_files(dirpath_, "csv")

    # Make list of times and sort it
    
    time_list = set()

    for path in list_of_files:
        if path:
            if "_" in path:
                time = path.rsplit('.')[0].rsplit('_')[1]
                time_list.add(dt.strptime(time, "%H:%M:%S"))

    time_list = sorted(time_list)

    date_dict[argument] = time_list

for date, time_list in date_dict.items():
    for time in time_list[1:]:
        time_s = dt.strftime(time, "%H:%M:%S")
        file = "{}/{}_{}.csv".format(date,date, time_s)
        frame = pd.read_csv(dirpath + file)
        frame['Time'] = dt.strftime(time, "%H:%M")
        frame['Date'] = date

        pieces.append(frame)

amfinav = pd.DataFrame()
# Concatenate everything into a single DataFrame
if pieces:
    amfinav = pd.concat(pieces, ignore_index=True)


# print(amfinav.head())
# print(amfinav.info())

total_update = amfinav.pivot_table('Scheme Code', index=['Scheme House'], columns=[
                                      'Time','Date'], aggfunc='count')

pd.options.display.float_format = '{:,.0f}'.format
total_update = total_update.fillna('-')

print(total_update)
