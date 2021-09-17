#!/usr/bin/env python3
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/..")

from datetime import datetime as dt, timedelta
from libs.ppsecurity import PpAmfi,PpNse,PpBse
from libs.ppstdlib import list_of_files

argLen = len(sys.argv)
fromdate = ""
todate = ""
choice = "*"
if argLen == 2:
	if sys.argv[1] in ["t","y"]:  #update today price
		todate = dt.now()
		if sys.argv[1] == "y":
			todate = todate - timedelta(1)
		todate = todate.strftime("%m%d%Y")
		fromdate = todate
	else:
		print("Enter Valid input. \"t\" for Today, \"y\" for Yesterday")
		print("if you wish to download prices for the range, please don't enter the arguments in the execution commands")
		print("system will get such information form you by an interactive way")
		sys.exit(0)
else:
	try:
		fromdate = dt.strptime(input("Enter From date MMDDCCYY: "), '%m%d%Y')
		todate = dt.strptime(input("Enter To date MMDDCCYY: "), '%m%d%Y')
		if todate < fromdate or todate > dt.now() :
			print("Enter Valid date range, your todate must be greater than or equal to from date and not to be a future date.")
			sys.exit(0)
		fromdate = fromdate.strftime("%m%d%Y")
		todate = todate.strftime("%m%d%Y")
		
		choice = input("which prices are you want to download [a:AMFI,n:NSE,b:BSE,*:ALL] \n")
		if choice not in ["a", "n", "b", "*"]:
			print("Enter Valid Choice.")
			sys.exit(0)
		
	except ValueError:
		print ("Please Enter the Valid date")
		sys.exit(0)
misspri = []
#-----------------------------------------------------------------------------------------------------
#                                       For AMFI
#-----------------------------------------------------------------------------------------------------

if choice in ["*", "a"]:
	ppamfi = PpAmfi(fromdate,todate)
	ppamfi.DLPrice()
	misspri = ppamfi.misspri
	del ppamfi
#-----------------------------------------------------------------------------------------------------
#                                       For NSE
#-----------------------------------------------------------------------------------------------------
if choice in ["*", "n"]:
	nseobj = PpNse()
	nseobj.suck_EQ_Hist(fromdate,todate)
	nseobj.save(isCopyRaw=True)
	nseobj.suck_B(fromdate,todate)
	nseobj.save(isCopyRaw=True,intoDir="bond")
	nseobj.suck_I(fromdate,todate)
	nseobj.save(isCopyRaw=True,intoDir="option")
	misspri = nseobj.misspri
	del nseobj
#-----------------------------------------------------------------------------------------------------
#                                       For BSE
#-----------------------------------------------------------------------------------------------------

if choice in ["*", "b"]:
	bseobj = PpBse()
	bseobj.suck_E(fromdate,todate)
	bseobj.save(isCopyRaw=True)
	bseobj.suck_B(fromdate,todate)
	bseobj.save(isCopyRaw=True,intoDir="bond")
	misspri = bseobj.misspri
	del bseobj

#---------------------------------------------------------------------------------------------------
#                           Unable to Download
#---------------------------------------------------------------------------------------------------
print("============================File Missing =================================================")
print(*misspri, sep = "\n")
del misspri

