#!/usr/bin/env python3
import sys, os
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/..")

from datetime import datetime as dt,timedelta
from libs.ppsecurity import PpAmfi
from libs.exceptions import *

class AmfiCron:
    do_by_modes = ['today','yesterday']
    def __init__(self, do_by='yesterday'):
        if do_by not in self.do_by_modes:
            print("Invalid doby mode option <{}>".format(do_by), self.do_by_modes)
            sys.exit()
        self.__now = dt.now()
        self.__download_date = self.__now.strftime("%m%d%Y")
        
        
        if do_by == 'today':
            self.__date_of_price = self.__now.strftime("%m%d%Y")
        else:
            self.__date_of_price = (self.__now - timedelta(1)).strftime("%m%d%Y")

    
    def fetch(self):    
        time = dt.now()

        print ("Doing stuff at {}".format(time))

        ppamfi = PpAmfi(self.__download_date)
        try:
            ppamfi.suck()
        except PpException as e:
            print(e.e_type)
            print(e.e_code)
            print(e.e_desc)

        ppamfi.update_log('mylogtext')
        ppamfi.parse()
        ppamfi.save(isCopyRaw=True,saveOnly=self.__date_of_price,includeTime=True,intoDir=self.__date_of_price)


if len(sys.argv) != 2:
    print('Empty argument[today|yesterday')
    sys.exit()

# print(sys.argv[1])

AmfiCron(sys.argv[1]).fetch()