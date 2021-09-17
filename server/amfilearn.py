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


pieces = []
dirpath = '/var/prism/Central/raw/amfi/{}/'.format(arguments[0])

# Get all csv files to process
list_of_files = list_of_files(dirpath, "csv")

# Make list of times and sort it
lab_count = 0
time_list = set()

for path in list_of_files:
    if path:
        if "_" in path:
            time = path.rsplit('.')[0].rsplit('_')[1]
            time_list.add(dt.strptime(time, "%H:%M:%S"))

time_list = sorted(time_list)



for time in time_list:
    time_s = dt.strftime(time, "%H:%M:%S")
    file = "{}_{}.csv".format(date, time_s)
    frame = pd.read_csv(dirpath + file)
    frame['Time'] = dt.strftime(time, "%H:%M")

    pieces.append(frame)


# Concatenate everything into a single DataFrame
if pieces:
    amfinav = pd.concat(pieces, ignore_index=True)
    # print(amfinav.head())

    # print(amfinav['Scheme House'].value_counts())

    schemes_by_house = amfinav.groupby('Scheme House').size()
    schemes1000 = schemes_by_house.index[schemes_by_house >= 1000]

    total_update = amfinav.pivot_table('Scheme Code', index=['Scheme House'], columns=[
                                       'Time'], aggfunc='count')

    # print(dt.strftime(time_list[0],"%H:%M:%S"))
    # print(dt.strftime(time_list[-1],"%H:%M:%S"))

# Get differnce last and last before
    total_update['diff'] = total_update[dt.strftime(time_list[0],"%H:%M")] - total_update[dt.strftime(time_list[-1],"%H:%M")]

    # print(total_update.info())
    pd.options.display.float_format = '{:,.0f}'.format
    print(total_update.fillna(''))
    print("----------------------------------------------------------------------")
    print("Totally {} fund houses are giving prices for overall {:,.0f} schemes/funds".format(len(total_update),
                            total_update[dt.strftime(time_list[0],"%H:%M")].sum() ))
    print("----------------------------------------------------------------------")