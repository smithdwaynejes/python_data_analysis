#!/usr/bin/env python3
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/..")

from datetime import datetime as dt, timedelta
from libs.ppsecurity import PpAmfi,PpNse,PpBse
from libs.exceptions import *

import pandas as pd

if len(sys.argv) != 3:
    print("Enter date of Holiday,for exchanges[1:AMFI,2:NSE,3:BSE,4:ALL]:")
    sys.exit()


arguments = sys.argv[1:]

print(arguments)
date = dt.strptime(arguments[0],"%m%d%Y")

day_before = date - timedelta(1)

date = date.strftime('%m%d%Y')
day_before = day_before.strftime('%m%d%Y')

print('Saving prices for {} from {}'.format(date,day_before))

pp_nse_obj = PpNse()
pp_amfi_obj = PpAmfi()
pp_bse_obj = PpBse()
pp_obj_list = [pp_nse_obj,pp_amfi_obj,pp_bse_obj]
# print(pp_obj_list)

if arguments[1] == '1':
    pp_obj_list.remove(pp_nse_obj)
    pp_obj_list.remove(pp_bse_obj)
elif arguments[1] == '2':
    pp_obj_list.remove(pp_amfi_obj)
    pp_obj_list.remove(pp_bse_obj)
elif arguments[1] == '3':
    pp_obj_list.remove(pp_amfi_obj)
    pp_obj_list.remove(pp_nse_obj)

# print(pp_obj_list)
print(day_before)
print(date)

for pp_obj in pp_obj_list:
    try:
        pp_obj.cpyPri(day_before,date)
        print('updated prices for {} from {}'.format(date,day_before))
    except PpException as e:
        print(e.e_type)
        print(e.e_code)
        print(e.e_desc)
        print(e.e_value)

    


