#!/usr/bin/env python3

import sys, os, gc
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/..")

from libs.pppri import pppri
from libs.pptable import PpTable,load_files
from libs.ppsettings import pp_settings
import numpy as np
import pandas as pd
from datetime import datetime as dt,timedelta
from shutil import copyfile
from libs.exceptions import *


class PriceUpdate:
	typedf = pd.DataFrame();
	secdf = pd.DataFrame();
	missPri = pd.DataFrame(); #Missing Price
	missFle = []
	sysCurrency = "rs" #System Currency
	
	def __init__(self):
		self.GetSysCurrency()
		self.LdType()
		self.LdSec()
		
	def GetSysCurrency(self):
		# Get System Currency from Currency Table
		curr_obj = PpTable("currency.inf")
		currdf = curr_obj.get_data()
		self.sysCurrency = currdf.loc[currdf['system'] == 'y'].iloc[0]['curr']
	
	def LdType(self):
		# Read Security Information Table
		type_obj = PpTable("type.inf")
		self.typedf = type_obj.get_data().replace(np.nan, '', regex=True)[['type','bysl']].drop_duplicates()
	
	def LdSec(self):
		# Read Security Type Information Table
		sec_obj = PpTable("sec.inf")
		self.secdf = sec_obj.get_data().replace(np.nan, '', regex=True)[['type','symbol','cusip','pool','oid','maturity','estmat']]

		# merging security with security type
		self.secdf = pd.merge(self.secdf,self.typedf,on='type',how='outer').dropna()
		self.secdf = self.secdf[self.secdf.bysl=="y"]
		
	def CpyYesterdayFile(self,priFle):
		dir_path = load_files("/var/prism/config/.prism.conf",":")['DATADIR']
		route = dir_path + load_files(dir_path+"App/Indexes/leveltype.idx",'\t')["pri"] + "/pri/"
		oldFle = route + (dt.strptime(priFle,"%m%d%Y") - timedelta(1)).strftime("%m%d%Y") + ".pri"
		if not os.path.exists(oldFle):
			print("File Not Found in the Path {}".format(oldFle))
		else:
			priFle = route + priFle + ".pri"
			print("copied {} into {}".format(oldFle,priFle))
			copyfile(oldFle, priFle)
	
	def PriUpdate(self,priFle):
		print("Update the Prices for the Date ",priFle)
			
		# connect Central Repository prices..
		proobj = pppri()
		proobj.connect(host = "central") # set 'local' for dev mode
		consPriFle = pd.DataFrame();
		optionPriFle = pd.DataFrame();
		
		proobj.read(priFle)  # Read Mutual Fund
		consPriFle = proobj.pri_df_raw
		
		proobj.read(priFle,exchange='nse')
		consPriFle = consPriFle.append(proobj.pri_df_raw, sort=False)
		
		proobj.read(priFle,exchange='nse',intoDir="bond")
		consPriFle = consPriFle.append(proobj.pri_df_raw, sort=False)
		
		proobj.read(priFle,exchange='nse',intoDir="option")
		optionPriFle = proobj.pri_df_raw
		if not optionPriFle.empty:
			#if the exchange are Nse and Bse then the currency Type is "rs"
			optionPriFle['type'] = optionPriFle['type'].replace(['XX', 'PE', 'CE'], ['furs', 'ptrs', 'clrs'])
			optionPriFle = pd.merge(self.secdf,optionPriFle,left_on=['type','pool','oid','maturity'], right_on=['type','symbol','strike','expiry'])
			optionPriFle = optionPriFle[['type','cusip','price']]
			optionPriFle = optionPriFle.rename(columns = {"cusip": "symbol"})
			consPriFle = consPriFle.append(optionPriFle, sort=False)
			del optionPriFle
			gc.collect()
				
		proobj.read(priFle,exchange='bse',intoDir="bond")
		consPriFle = consPriFle.append(proobj.pri_df_raw, sort=False)
		
		proobj.read(priFle,exchange='bse')
		consPriFle = consPriFle.append(proobj.pri_df_raw, sort=False)
		
		#Update Price File
		if not consPriFle.empty:
			consPriFle.drop_duplicates(subset ="symbol", inplace = True)
			consPriFle['symbol'].replace('', np.nan, inplace=True)
			consPriFle.dropna(subset=['symbol'], inplace=True)
					
			# consPriFle = consPriFle.rename(columns = {"symbol": "cusip"})		
			consPriFle = pd.merge(self.secdf,consPriFle,left_on='cusip', right_on='symbol',how='outer',indicator='merge')
			self.missPri = consPriFle[consPriFle["merge"]=="left_only"]
			consPriFle = consPriFle[consPriFle["merge"]=="both"]
			if not consPriFle.empty:
				# print("Removed UnWanted Columns")
				# consPriFle.drop(consPriFle.columns[[2, 3, 4, 5, 6, 7, 8, 10,11]], axis=1, inplace=True)
				consPriFle.drop(consPriFle.columns.difference(['type_x','symbol_x','price']), axis=1, inplace=True)
				# self.missPri = consPriFle[consPriFle["merge"]=="left_only"].drop(labels=['merge','price'], axis=1) 
				
				if not self.missPri.empty:
					#Read one Day Earlier Price
					try:
						for index,row in self.missPri.iterrows():
							if row['type_x'] [:2] in proobj.settings['mature']:
								if row['maturity'].strip() != "" :
									if pd.to_datetime(row['maturity'],format="%m%d%Y") < dt.strptime(priFle,"%m%d%Y") :
										self.missPri.drop(index, inplace=True)
							else:
								# This section block prevent us to carring the debared securitied (or)
								# Need not carry old security code price after merge
								if row['estmat'].strip() != "":
									if pd.to_datetime(row['estmat'],format="%m%d%Y") < dt.strptime(priFle,"%m%d%Y") :
										self.missPri.drop(index, inplace=True)
								
						# self.missPri.drop(self.missPri.columns[[2, 3, 4, 5, 6, 7, 8, 9, 10, 11]], axis=1, inplace=True)
						self.missPri.drop(self.missPri.columns.difference(['type_x','symbol_x']), axis=1, inplace=True)
						priobj = PpTable((dt.strptime(priFle,"%m%d%Y") - timedelta(1)).strftime("%m%d%Y") + ".pri")
						oneDayEPri = priobj.get_data()
						missing = pd.merge(self.missPri,oneDayEPri,left_on=['type_x','symbol_x'], right_on=['type','symbol']).drop(labels=['type','symbol'], axis=1) 
						consPriFle = consPriFle.append(missing, sort=False)
					except PpException as e:
						print("{} - {}".format(e.e_desc,e.e_value))
					
				pri_obj = PpTable('%s.pri' %priFle,create_if_not_exist=True)
				pFle = pri_obj.get_data()
				pri_obj.save_data(consPriFle,mode="w")
				del consPriFle
				gc.collect()
			else:
				self.CpyYesterdayFile(priFle)
		else:
			self.CpyYesterdayFile(priFle)
	
		self.missFle = proobj.misspri
		del proobj
		
	def UpdatePrices(self,endDte,startDte=""):
		# print("system currency ")
		# print(self.sysCurrency)
		# print("Type Information")
		# print(self.typedf.to_string())
		# print("sec Information")
		# print(self.secdf.to_string())
		
		try:
			endDte = dt.strptime(endDte, '%m%d%Y')
			if endDte > dt.now():
				print("you can not choose Furure date.")
				sys.exit(0)
				
			if startDte:
				startDte = dt.strptime(startDte, '%m%d%Y')
				inc = timedelta(1)
				while startDte <= endDte:
					priFle = startDte.strftime("%m%d%Y")
					self.PriUpdate(priFle)
					startDte += inc
			else:
				priFle = endDte.strftime("%m%d%Y")
				self.PriUpdate(priFle)
		except ValueError:
			print ("Please Enter the Valid date")
			
			

argLen = len(sys.argv)
endDate = ""
startDte = ""
if argLen == 1:
	todate = dt.now()
	endDate = todate - timedelta(1)
	endDate = endDate.strftime("%m%d%Y")
elif argLen == 2:
	endDate = sys.argv[1]
elif argLen == 3:
	startDte = sys.argv[1]
	endDate = sys.argv[2]
	
uPrice = PriceUpdate()
uPrice.UpdatePrices(endDate,startDte)
print("Missing Price for the Security")
print(uPrice.missPri.to_string())
print("===================================")
print("Files Missing")
print(*uPrice.missFle, sep = "\n")
# del uPrice.missFle
gc.collect()

