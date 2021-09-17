#!/usr/bin/env python3
import sys, os
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/..")

from libs.ppsecurity import PpAmfi

from datetime import datetime as dt,timedelta




def auto_amfi(todate,datefor):
    time = dt.now()

    print ("Doing stuff at {}".format(time))

    ppamfi = PpAmfi(todate)
    ppamfi.suck()
#   ppamfi.read("/home/infant/20190225.txt")
    ppamfi.parse()
    ppamfi.save(isCopyRaw=True,saveOnly=datefor,includeTime=True)
#   ppamfi.save(isCopyRaw=True,saveOnly="03052019",includeTime=True)
    lastupdateFile = ppamfi.settings['path']['raw'] + ppamfi.settings['lastupdate']
    if ppamfi.rawoutput != "":
        with open(lastupdateFile,"w+") as fp:
            fp.writelines(ppamfi.rawoutput)

human_time = dt.now()
todate = human_time.strftime("%m%d%Y")
yesterday = human_time - timedelta(1)
yesterday = yesterday.strftime("%m%d%Y")


if len(sys.argv) != 3:
    print("Enter date of download and datafor:")
    sys.exit(1)

arguments = sys.argv[1:]
todate = arguments[0]
yesterday = arguments[1]
auto_amfi(todate,yesterday)
