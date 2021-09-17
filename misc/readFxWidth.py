#!/usr/bin/env python3

import sys, os
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/..")

from libs.pptable import PpTable
import numpy as np
import pandas as pd

sec_obj = PpTable("holidayb.inf")

sec_data =sec_obj.getdata()
mystr = " ".join("%"+str(i)+"s" for i in sec_obj.dfidx.fsize.values)
print(sec_obj.dfidx.fcode.values)
print(mystr)

with open('output.dat',"w+") as ofile:
     np.savetxt(ofile, sec_data.values, fmt=mystr)
