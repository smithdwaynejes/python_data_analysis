#!/usr/bin/env python3
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/..")

from datetime import datetime as dt, timedelta
from libs.ppsecurity import PpAmfi,PpNse
from libs.ppstdlib import list_of_files

if len(sys.argv) != 3:
	print("Enter source path and date to update:")
	sys.exit(0)
	
source = sys.argv[1]
date = sys.argv[2]
# print('Enter file to update...')
# source = input()
# print('Enter date to update...')
# date = input()

obj = PpAmfi()
obj.read(source)
# obj.read('/var/prism/Central/raw/amfi/core/04072019_08:00:03.txt')
obj.parse()
obj.save(saveOnly=date)
