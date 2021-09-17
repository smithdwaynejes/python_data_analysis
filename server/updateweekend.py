#!/usr/bin/env python3
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/..")

from datetime import datetime as dt, timedelta
from libs.ppsecurity import PpAmfi,PpNse
from libs.exceptions import *

import pandas as pd

monday = dt.now() - timedelta(1)
sunday = monday - timedelta(1)
saturday = monday - timedelta(2)
friday = monday - timedelta(3)


friday = friday.strftime("%m%d%Y")
saturday = saturday.strftime("%m%d%Y")
sunday = sunday.strftime("%m%d%Y")
monday = monday.strftime("%m%d%Y")


#-----------------------------------------------------------------------------------------------------
#                                       For AMFI
#-----------------------------------------------------------------------------------------------------


""" ppamfi = PpAmfi(saturday,monday)
ppamfi.suck()
ppamfi.parse()
ppamfi.save(isCopyRaw=True) """

 
""" ppamfi = PpAmfi()
# friday = "03322018"
try:
    ppamfi.copy_price(friday,saturday)
    ppamfi.copy_price(saturday,sunday)
except PpException as e:
    print(e.e_type)
    print(e.e_code)
    print(e.e_desc)
    print(e.e_value) """


#-----------------------------------------------------------------------------------------------------
#                                       For NSE
#-----------------------------------------------------------------------------------------------------


""" ppamfi = PpNse()
friday = dt.strptime('03152019',"%m%d%Y")
saturday = friday + timedelta(1)
sunday = friday + timedelta(2)

friday = friday.strftime('%m%d%Y')
saturday = saturday.strftime('%m%d%Y')
sunday = sunday.strftime('%m%d%Y')

print(friday)
print(saturday)
print(sunday)

try:
    ppamfi.copy_price(friday,saturday)
    ppamfi.copy_price(saturday,sunday)
except PpException as e:
    print(e.e_type)
    print(e.e_code)
    print(e.e_desc)
    print(e.e_value) """

""" friday_df = pd.read_csv('/var/prism/Central/pp/amfi/03152019.csv',delimiter=' *, *', engine='python',
								keep_default_na=False, dtype=str)

saturday_df = pd.read_csv('/var/prism/Central/pp/amfi/03162019.csv',delimiter=' *, *', engine='python',
								keep_default_na=False, dtype=str)

sunday_df = pd.read_csv('/var/prism/Central/pp/amfi/03172019.csv',delimiter=' *, *', engine='python',
								keep_default_na=False, dtype=str)

friday_df.sort_values(['symbol']).to_csv("03152019.csv", mode="w+", index=False)

saturday_df.sort_values(['symbol']).to_csv("03162019.csv", mode="w+", index=False)

sunday_df.sort_values(['symbol']).to_csv("03172019.csv", mode="w+", index=False)
  """

monday = dt.now()
sunday = monday - timedelta(1)
saturday = monday - timedelta(2)
friday = monday - timedelta(5)


friday = friday.strftime("%m%d%Y")
saturday = saturday.strftime("%m%d%Y")
sunday = sunday.strftime("%m%d%Y")
monday = monday.strftime("%m%d%Y")
print(monday)
print(friday)
#-----------------------------------------------------------------------------------------------------
#                                       For AMFI
#-----------------------------------------------------------------------------------------------------


amfiobj = PpAmfi(friday,monday)
amfiobj.suck()
amfiobj.parse()
amfiobj.save(isCopyRaw=True) 


#-----------------------------------------------------------------------------------------------------
#                                       For NSE
#-----------------------------------------------------------------------------------------------------


# nseobj = PpNse(todate)
# nseobj.suck()
# # ppamfi.parse()
# nseobj.save(isCopyRaw=True, saveOnly=yesterday) 
