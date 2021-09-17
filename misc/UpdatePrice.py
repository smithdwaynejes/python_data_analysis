#!/usr/bin/env python3
import sys, os
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/..")

from libs.ppreadssh import param_ssh
from libs.ppsettings import pp_settings
from libs.pptable import PpTable

import numpy as np
import pandas as pd

import requests
import sys
import csv




class MakePrices:
	priceList = []
	nonUpdateList = []
	sourcedf=pd.DataFrame()
	# empty constructor 
	def __init__(self, sourcedf):
		self.sourcedf = sourcedf
		
	def makeprices(self,secdf):
		for row in secdf.iterrows()[1]:
			# print(row['symbol'])
			if row.symbol in sourcedf.groups:
				self.priceList.append([k,row.symbol,str(sourcedf.get_group(row.symbol)['CLOSE_PRICE'].iloc[0]) ])
			else:
				self.nonUpdateList.append([k,row.symbol,""])
	
if len (sys.argv) != 4 :
    print ("Usage: Date to be update, Type of Security[e-Equities,m-Mutual Fund, Name of Price Table")
    sys.exit (1)

arguments = sys.argv[1:]

fdate = arguments[0]
sectype = arguments[1]
price = arguments[2] + ".pri"

sourcepath = ""

# loading Prims Portfolio Settings
print("loading PP Settings...")
settings = pp_settings()

if sectype == "e":
	sourcepath = settings['equity-path']
	sourcepath = sourcepath + fdate + ".csv"

# Set Central Repository connections
address=settings['central-host']
username=settings['central-user']
password=settings['central-pass']


# Connect with Central Repository
print("Connecting Central Repo...")
ssh_client = param_ssh(address,username,password)
sftp_client = ssh_client.open_sftp()
print("Connected Central Repo Successfully...")

# type_obj = PPTable("type.inf")
# typedf = type_obj.getdata().replace(np.nan, '', regex=True)[['type','bysl','name']].groupby('type')
# # typedf = openTable("type.inf")

# sec_obj = PPTable("sec.inf")
# secdf = sec_obj.getdata().replace(np.nan, '', regex=True)[['type','symbol','name']].groupby('type')

# Read Security Information Table
type_obj = PpTable("type.inf")
typedf = type_obj.getdata().replace(np.nan, '', regex=True)[['type','bysl']].drop_duplicates()

# Read Security Type Information Table
sec_obj = PpTable("sec.inf")
secdf = sec_obj.getdata().replace(np.nan, '', regex=True)[['type','symbol']]


# merging security with security type
secdf = pd.merge(secdf,typedf,on='type',how='outer')
# print(secdf)
# removing all null rows at symbol column
secdf = secdf[pd.notnull(secdf['symbol'])]

onlybysldf = secdf.groupby(['bysl']).get_group('y')


# make empty dataframe for handle securities from Central Repo
sourcedf = pd.DataFrame() #creates a new dataframe that's empty

# Read csv from Central Repository
print("Reading " + sourcepath + " ... ")


try:
	sftp_client.stat(sourcepath)
	print(sourcepath + ' exists ...')
	sourcedf = pd.read_csv(sftp_client.open(sourcepath)).rename(columns=lambda x: x.strip()).groupby('SYMBOL')
	print("loaded NSE prices from Central Repository...")
except IOError:
	
	# download NSE Daily Price List from NSE
	print("Downloading NSE Prices for NSE....")
	url="https://www.nseindia.com/products/content/sec_bhavdata_full.csv"
	data=requests.get(url)

	# nsecsvdf = pd.read_csv("")
	# Saving downloaded csv to local directory 
	print("Saving NSE Prices at Local directory....")

	with open(fdate +'.csv', 'w') as f:
		writer = csv.writer(f)
		reader = csv.reader(data.text.splitlines())
		for row in reader:
			writer.writerow(row)

	print("Uploading downloaded csv to "+ sourcepath +" at remote server....")
	sftp_client.put(fdate +'.csv', sourcepath)
	
	sourcedf = pd.read_csv(fdate +'.csv').rename(columns=lambda x: x.strip()).groupby('SYMBOL')
	print("loaded NSE prices from loacl path...")
	


priobj = MakePrices(sourcedf)

# print()

if sectype == "e":
	onlybysldf = onlybysldf[~onlybysldf.type.str.contains("mf")]
elif sectype == "m":
	onlybysldf = onlybysldf[~onlybysldf.type.str.contains("mf")== False]
	
for k,v in onlybysldf.groupby('type'):
		priobj.makeprices(v)

print(priobj.priceList)
# print(nsedf.groups)
# for k,v in nsedf:
	# print(k)
	# print(v.columns)
	# print("-------")


# p_i = 0
# p_ni= 0
# for k,v in secdf:
	# if k in typedf.groups:
		# # print(k)
		# if "mf" not in k and typedf.get_group(k)['bysl'].iloc[0] == "y":
			# for symbol in v.symbol:
				# if symbol in nsedf.groups:
					# 
					# p_i = p_i + 1
				# else:
					# nonUpdateList.append([k,symbol,""])
					# p_ni = p_ni + 1
		
# print(priceList)
