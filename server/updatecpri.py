#!/usr/bin/env python3
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/..")

from datetime import datetime as dt, timedelta
from libs.ppsecurity import PpAmfi,PpNse,PpBse
from libs.ppstdlib import list_of_files



todate = dt.now()
yesterday = todate - timedelta(1)
todate = todate.strftime("%m%d%Y")
yesterday = yesterday.strftime("%m%d%Y")

#-----------------------------------------------------------------------------------------------------
#                                       For AMFI
#-----------------------------------------------------------------------------------------------------

ppamfi = PpAmfi(todate)
ppamfi.suck()
ppamfi.parse()
ppamfi.save(isCopyRaw=True, saveOnly=yesterday)

#-----------------------------------------------------------------------------------------------------
#                                       For NSE
#-----------------------------------------------------------------------------------------------------

 
nseobj = PpNse(todate)
nseobj.suck()
nseobj.save(isCopyRaw=True, saveOnly=yesterday)
nseobj.suck_B(yesterday)
nseobj.save(isCopyRaw=True, saveOnly=yesterday, intoDir='bond')
nseobj.suck_I(yesterday)
nseobj.save(isCopyRaw=True, saveOnly=yesterday, intoDir='option')

#-----------------------------------------------------------------------------------------------------
#                                       For BSE
#-----------------------------------------------------------------------------------------------------

bseobj = PpBse(todate)
bseobj.suck_E(yesterday)
bseobj.save(isCopyRaw=True, saveOnly=yesterday)
bseobj.suck_B(yesterday)
bseobj.save(isCopyRaw=True, saveOnly=yesterday, intoDir='bond')
