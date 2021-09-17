#!/usr/bin/env python3

import sys, os
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/..")

from libs.ppsecurity import PpAmfi,PpNse

ppamfi = PpAmfi("02202019")
# ppamfi.suck()
ppamfi.read("/home/heartly/20190220.txt")
ppamfi.parse()
ppamfi.save(isCopyRaw=False,saveOnly="02202019",includeTime=True,intoDir='02202019')

# ppnse = PPnse("02202019")
# ppnse.suck()
# ppnse.save(doDiff=True,isCopyRaw=True,saveOnly="02202019")
