#!/usr/bin/env python3

import sys, os
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/..")


import requests
from bs4 import BeautifulSoup as bs
from datetime import datetime, timedelta
from ppsettings import pp_settings
import sys
import os 
import pandas as pd
import re

class amfi:
	
	__url=""
	__fdate = ""
	__edate = ""
	__soup = bs("","lxml")
	__mode = 'c'
	__modes = ['c','h']
	
	dataPass = False
	
	# Emtpy data frame
	__mfdf = pd.DataFrame()
	def __init__(self,url=""):
		self.url = url
		self.dataPass = False
		
		# loading Prims Portfolio Settings
		self.settings = pp_settings()
		self.__columns = self.settings['amfi-current']['columns']
		
	def downladHist(self,edate,fdate=""):
		

		# Initialize urls from settings
		urlperiod = self.settings['amfi-hist']['url']
		urlcurrent = self.settings['amfi-current']['url']
		
		# setting up from and to date
		self.__edate = datetime.strptime(edate, '%m%d%Y').strftime("%d%m%Y")
		self.url = urlcurrent + self.__edate
		
		if fdate:
			self.__fdate = datetime.strptime(edate, '%m%d%Y').strftime("%d-%b-%Y")
			self.__edate = datetime.strptime(fdate, '%m%d%Y').strftime("%d-%b-%Y")
			self.url = urlperiod + self.__fdate + "&todt=" + self.__edate
			# navhist = amfi(31-Jan-201901-Feb-2019")

		print("downloading prices from " + self.url + "....")
		self.__soup = bs(requests.get(self.url).text,"html.parser")
	
	def load(self):
		if self.url:
			if os.path.exists(self.url):
				with open(self.url) as f:
					self.__soup = bs(f.read(),"lxml")
			else:
				print("File not found...")
				sys.exit()
		else:
			print("undefined url..")
			sys.exit()
	
	
	def getbyline(self):
		# print(self.__fdate)
		# print(self.__edate)
		# print(self.url)
		if self.__soup:
			return self.__soup.text.split("\r\n")
		return []
	
	def getplaintext(self):
		return self.__soup.text
	
	def parse(self,mode="c",delim=';'):
		if mode:
			if mode not in self.__modes:
				print("Invalid mode option![Current,Historical]", self.__modes)
				sys.exit()
		
		self.__mode = mode
		
		# setup columns depends on parsing mode
		if self.__mode != 'c':
			self.__columns = self.settings['amfi-hist']['columns']
		
		
		# print(self.__columns)
		scheme_type = ""
		scheme_category = ""
		scheme_house = ""
		scheme_type_list = ["Open", "Close","Interval"]
		dtable =[]
		if self.__soup.text:
			allLines = self.__soup.text.split("\n")
			
			# set column from the source data
			if delim in allLines[0]:
				self.__columns = allLines[0].split(delim)
			
			# parse data lines one by one
			for line in allLines[1:]:
				if line.strip():								# check if line is empty
					if delim in line:							# checking the line is data row
						row = line.split(delim)					# Split data line into a list by delim
						row.append(scheme_type)
						row.append(scheme_category)
						row.append(scheme_house)
						dtable.append(row)
					else:
						if '(' in line and ')' in line and any(stype in line for stype in scheme_type_list):			# checking the line is Scheme Type and Scheme category
							scheme_type = line.split('(', 1)[0].strip()
							scheme_category =line.split('(', 1)[1].split(')')[0].strip()
						else:
							scheme_house = line
		# Extend columns for 3 extra columns
		self.__columns.extend(["Scheme Type","Scheme Category","Scheme House"])
		
		# Make dataframe on parsing mode
		self.__mfdf = pd.DataFrame(dtable,columns=self.__columns) 
		
		# Change columns names as per uniform
		self.__mfdf=self.__mfdf.rename(columns = {'Scheme Code':'symbol','Net Asset Value':'price','Date':'date'})
		
		self.dataPass = True

	def makecsvbydate(self,onlydiff):
		
		update_count = 0
		if not self.__mfdf.empty:
			print(self.__mfdf.columns.values)
			# self.__mfdf.price = pd.to_numeric(self.__mfdf.price,errors='coerce')
			
			# print(self.__mfdf.groupby('Date').keys)
			
			
			for k, df_new in self.__mfdf.groupby('date'):
				df_new =  df_new[['symbol','price']].reset_index(drop=True)
				dateKey = datetime.strptime(k,"%d-%b-%Y").strftime("%Y%m%d")
				
				csvpath = "pri/"+dateKey+".csv"
				
						
				if onlydiff:
					
					df_exist = pd.DataFrame()
					append_write = 'a'# append if already exists
					header = False
					if not os.path.exists(csvpath):
						header = True
						open(csvpath,"w") # make a new file if not
					else:
						df_exist = pd.read_csv(csvpath,dtype={'symbol': object,'price':object})
						
					if not df_exist.empty:
						gradeBool = ( df_exist!=df_new).stack()
						df_new = pd.concat([df_exist.stack()[gradeBool],
								df_new.stack()[gradeBool]], axis=1)

				if not df_new.empty:
					update_count = update_count + 1
					df_new.to_csv(csvpath,mode = append_write, columns=['symbol','price'],index=False,header = header)
				# print(df)

			print("Prices updated sucessfully for " + str(update_count) + " dates...")
		
		
navhist = amfi("20190201.txt")
navhist.load()
navhist.parse(mode='c')
if navhist.dataPass:
	print("Data parsed successfully...")
	navhist.makecsvbydate(onlydiff=True)


# df_new = pd.DataFrame([['apple','100.5'],['reliance','55.5'],['tata','20.1']],columns=['symname','nav'])
# print(df_new)

# df_exist = pd.DataFrame([['apple','100.5'],['tata','20.1']],columns=['sym','nav'])
# print(df_exist)

# merged = df_exist.merge(df_new, indicator=True, how='outer')
# print(merged[merged['_merge'] == 'right_only'])

# navhist = amfi()
# navhist.downladHist("01302019","01312019")
# navhist.parse("c")

# navhist = amfi("01302019","01312019")
# navhist.getbyline()
# navhist.downladHist()
# allLines = navhist.getbyline()
# print(allLines)


# navhist = amfi("02022016")


# current_date = datetime.now()
# current_quarter = round((current_date.month - 1) / 3 + 1)
# first_date = datetime(2010, 1, 1)
# last_date = datetime(2010, 2, 1) + timedelta(days=-1)

# print("First Day of Quarter:", first_date)
# print("Last Day of Quarter:", last_date)
