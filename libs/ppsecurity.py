#!/usr/bin/env python3

from libs.ppsettings import pp_settings

from libs.pptable import PpTable
from libs.ppgetallweekend import get_all_week_ends
from libs.exceptions import RequestError, CopyError
from libs.ppstdlib import create_dir_if_not

import pandas as pd
from pandas import ExcelWriter
from datetime import datetime as dt, timedelta
from bs4 import BeautifulSoup as bs
import io
import requests
import os
import sys,gc
from shutil import copyfile
import zipfile



def diff_df(df_source, df_dest, compare_cols=[]):
	# try:
	# print(compare_cols)
	if compare_cols:
		if not df_source.empty:

			# make a new column compare for both data frame with the by making the column values into one
			if len(compare_cols) == 1:
				df_source['compare'] = df_source[compare_cols]
				df_dest['compare'] = df_dest[compare_cols]
			else:
				df_source['compare'] = df_source[compare_cols].apply(
					lambda x: ''.join(x), axis=1)
				df_dest['compare'] = df_dest[compare_cols].apply(
					lambda x: ''.join(x), axis=1)

			# find all difference securities from new to existence
			new_symbol = set(df_source.groupby('compare').groups.keys(
			)) - set(df_dest.groupby('compare').groups.keys())

			""" print(len(df_dest.groupby('compare').groups.keys()))
			print(len(df_source.groupby('compare').groups.keys()))

			print(new_symbol) """
			# selecting only diffent securites
			df_source = df_source[df_source['compare'].isin(list(new_symbol))]

			# drop the compare column
			df_source = df_source.drop('compare', axis=1)
		""" else:
			# Handle exception
	except:
		# my exception message
	else: """
	return df_source


def save_if(filename, df_new, compare_cols=[], doDiff=True, ignoreExist=False):

	if ignoreExist:
		if os.path.exists(filename):
			print("{} already exist. Ignoring save....".format(filename))
			return

	header = True
	append_write = 'w+'
	if doDiff:
		df_exist = pd.DataFrame()
		append_write = 'a'  # append if already exists
		header = False
		if not os.path.exists(filename):
			header = True
			open(filename, "w")  # make a new file if not
		else:
			df_exist = pd.read_csv(filename, dtype=str, keep_default_na=False)
			df_new = diff_df(df_new, df_exist, compare_cols)

	if not df_new.empty:
		df_new.to_csv(filename, mode=append_write, index=False, header=header)
		print("data has been saved successfully into {}".format(filename))
		return filename
	return ""


def load_data_frame(filename, hardcheck=False):
	df = pd.DataFrame()
	try:
		if os.path.exists(filename):
			df = pd.read_csv(filename, dtype=str, keep_default_na=False)
		else:
			if hardcheck:
				raise OSError
			else:
				return df
	except OSError:
		print('Source Path not found:', filename)
		raise
	else:
		return df

# parent class


class PpSecurity:
	url = ""
	columns = []
	settings = {}
	exchange = ""
	main_df = pd.DataFrame()
	rawoutput = ""
	ppoutput = ""
	callPriFunc = ""
	misspri = []

	def set_url(self, url):
		if not url:
			raise Exception('undefined url')
		self.url = url
		# print(self.url)

	def __init__(self, edate, fdate=""):

		self.settings = pp_settings()[self.exchange]

		url = self.settings["url"]["current"].format(
			dt.strptime(edate, '%m%d%Y').strftime("%d%m%Y"))

		if fdate != "":
			fdate, edate = edate, fdate
			url = self.settings["url"]["hist"].format(dt.strptime(fdate, '%m%d%Y').strftime("%d-%b-%Y"),
													  dt.strptime(edate, '%m%d%Y').strftime("%d-%b-%Y"))

		print(url)
		self.set_url(url)

	def copy_price(self, source, dest,intoDir=''):

		source = self.settings['path']['pp']+intoDir+source+".csv"
		dest = self.settings['path']['pp']+intoDir+dest+".csv"
		if not os.path.exists(source):
			raise CopyError('701', source)
		source_df = pd.read_csv(source, delimiter=' *, *', engine='python',
								keep_default_na=False, dtype=str)

		if not os.path.exists(dest):
			# adding exception handling
			try:
				copyfile(source, dest)
			except IOError as e:
				raise CopyError('703', e)
			except:
				raise CopyError('703', sys.exc_info())
			
		else:
			dest_df = pd.read_csv(dest, delimiter=' *, *', engine='python',
								keep_default_na=False, dtype=str)
			
			# print(dest_df.info())
			# print(source_df[~source_df.symbol.isin(dest_df.symbol)].info())
			dest_df = pd.concat([dest_df,source_df[~source_df.symbol.isin(dest_df.symbol)]],ignore_index=True)
			dest_df.to_csv(dest, mode="w+", index=False)
			# print(dest_df.info())
			# print(source_df.info())

		# pd.concat([sat, fri[~fri.symbol.isin(sat.symbol)]], ignore_index=True)

		

		# print()
		# print(self.settings['path']['pp']+dest+".csv")



	"""
	In the event of a network problem (e.g. DNS failure, refused connection, etc), Requests will raise a ConnectionError exception.

	In the event of the rare invalid HTTP response, Requests will raise an HTTPError exception.

	If a request times out, a Timeout exception is raised.

	If a request exceeds the configured number of maximum redirections, a TooManyRedirects exception is raised.

	All exceptions that Requests explicitly raises inherit from requests.exceptions.RequestException
	"""

	def request_url(self, url, stream = False):
		print("Downloading from {}".format(url))
		try:
			if not stream:
				r = requests.get(url)
				r.raise_for_status()
				return r.content
			else:
				r = requests.get(url,stream=True)
				r.raise_for_status()
				return r
		except requests.exceptions.ConnectionError as e:
			print("connection error {} ".format(url))
			raise RequestError('401', url)
		except requests.exceptions.HTTPError as e:
			print("HTTP error {} ".format(url))
			self.misspri.append(url);
			raise RequestError('402', url)
		except requests.exceptions.Timeout as e:
			print("Timeout error {} ".format(url))
			raise RequestError('403', url)
		except requests.exceptions.TooManyRedirects as e:
			print("Too Many Redirects error {} ".format(url))
			raise RequestError('404', url)
		except requests.exceptions.RequestException as e:
			print("Request Exception error {} ".format(url))
			raise RequestError('405', url)

	def show(self):
		print(self.main_df)

	def save(self, isCopyRaw=True, doDiff=False, saveOnly="", includeTime=False,intoDir = ''):

		print("Saving prices ....")
		if self.main_df.empty:
			print("The Downloaded file doesn't contains records")
			return
		
		# raw_df_columns = []
		raw_df_columns=self.settings["ppformat"].keys()
		
		for k, df_new in self.main_df.groupby(self.settings["saveby"]):
			df_new=df_new.reset_index(drop=True)
			dateKey=dt.strptime(
				k.strip(), "%d-%b-%Y").strftime("%m%d%Y")
			if saveOnly != "":
				if (dateKey == saveOnly):
					pass
				else:
					continue
			rawPath=self.settings["path"]['raw']
			ppPath=self.settings["path"]['pp']


			if intoDir != '':
				rawPath += intoDir
				ppPath += intoDir
				create_dir_if_not(rawPath)
				create_dir_if_not(ppPath)
				rawPath += '/'
				ppPath += '/'
				

			rawPath += dateKey
			ppPath += dateKey
			
			if includeTime:
				timestr=dt.now().strftime("%H:%M:%S")
				rawPath += "_" + timestr
				ppPath += "_" + timestr
			
			if isCopyRaw:
				self.rawoutput=save_if(rawPath + ".csv", df_new, doDiff, ignoreExist=True)
			self.ppoutput=save_if(ppPath+".csv", df_new[list(raw_df_columns)].rename(columns=self.settings["ppformat"]),
				   self.settings['duplicate'], doDiff)

	def fill_holiday(self, edate):
		destpath=self.settings["path"]['pp'] + edate + ".csv"
		edate=dt.strptime(edate, '%m%d%Y')

		adjustment={5: -1, 6: -2}.get(edate.weekday())
		if adjustment:
			edate += timedelta(days=adjustment)

		df_exist=load_data_frame(
			self.settings["path"]['pp'] + edate.strftime("%m%d%Y") + ".csv", hardcheck=True)

		# print(destpath)
		header=True
		df_new=pd.DataFrame()
		if os.path.exists(destpath):
			header=False

		df_new=load_data_frame(destpath)

		if df_new.empty:
			df_new=df_exist
		else:
			df_new=diff_df(df_exist, df_new, self.settings['duplicate'])
		if not df_new.empty:
			df_new.to_csv(destpath, mode='a+', index=False, header=header)
			print("data has been saved successfully into {}".format(destpath))

	def read_csv(self, path):
		self.main_df=pd.read_csv(path, delimiter=' *, *', engine='python', keep_default_na=False,
											dtype=str)
		# self.main_df = self.main_df.str.strip()

	def make_spread_sheet(self, sourcedf=None, storein="", groupby="", sheetname=""):

		sourcedf=self.main_df if sourcedf is None else sourcedf
		storein="output" if storein == "" else storein
		sheetname="sheet" if sheetname == "" else sheetname

		xlwriter=ExcelWriter(storein+'.xlsx')

		if not groupby:
			sourcedf.to_excel(xlwriter, sheetname)
		else:
			class MyError(Exception):
  				# Constructor or Initializer
				def __init__(self, value):
					self.value=value

				# __str__ is to print() the value
				def __str__(self):
					return(repr(self.value))

			try:
				if groupby not in sourcedf.columns:
					raise(MyError(groupby))

			# Value of Exception is stored in error
			except MyError as error:
				print('A New Exception occured: ', error.value)
				sys.exit()
			else:
				if not sourcedf.empty:
					sourcedf=sourcedf.groupby(groupby)
					for key in sourcedf.groups:
						sourcedf.get_group(key).to_excel(xlwriter, key)
					xlwriter.save()
					print("{} has been created successfully...".format(storein))
				else:
					return []


		if groupby:
			return sourcedf.groups
		return []

	def update_log(self,log_string=''):
		logfile = self.settings['path']['raw'] + self.settings['log']
		with open(logfile,'a+') as fp:
			fp.write('{}\t{}\n'.format(dt.now().strftime('%m%d%Y %H:%M:%S'),self.url))
			
	def clrDF(self):
		if not self.main_df.empty:
			del self.main_df
			gc.collect()
			self.main_df = pd.DataFrame()
			
	def UnZipBB(self,edate):
		#UnZip Bse Bond
		bdf = pd.DataFrame()
		r = self.request_url(self.url,True)
		if r.ok :
			zip_ref = zipfile.ZipFile(io.BytesIO(r.content))
			for zipinfo in zip_ref.infolist():
				flName = zipinfo.filename
				df = pd.read_csv(zip_ref.open(flName)).dropna(axis='columns', how = "all")
				if flName[:3] == "wdm":
					df = df.rename(columns ={"Scrip Code": "Security_cd",
												"Close Price": "LTP"})
					df['ISIN No.'] = df.apply(lambda row: row.Security_cd , axis = 1)
				elif flName[:4] == "icdm":
					df = df.rename(columns ={"Security Code":"Security_cd", 
											"Face Value": "FACE VALUE"})
				elif flName[:6] == "fgroup":
					df['LTP'] = df.apply(lambda row: (row['Close Price']/row['FACE VALUE'])*100 , axis = 1)
					
				df['TRADING_DATE'] = edate.strftime('%d-%b-%Y')
				bdf = bdf.append(df,sort=False)
			zip_ref.close()
			return bdf
		else :
			print ("Unable to unzip the Http response {}".format(self.url))
	
	def Unzip(self,flName):
		r = self.request_url(self.url,True)
		if r.ok :
			zip_ref = zipfile.ZipFile(io.BytesIO(r.content))
			df = pd.read_csv(zip_ref.open(flName)).dropna(axis='columns', how = "all")
			zip_ref.close()
			return df
		else :
			print ("Unable to unzip the Http response {}".format(self.url))
		
	def NseF(self,edate):
		url = "https://archives.nseindia.com/content/historical/DERIVATIVES/{}/{}/{}.zip";
		mon = edate.strftime("%^b")
		flName = "fo{}{}{}bhav.csv".format(edate.strftime("%d"),mon,edate.year);
		self.url = url.format(edate.year,mon,flName)
		dframe = self.Unzip(flName)
		self.main_df = self.main_df.append(dframe,sort=False)
		
	def NseB(self,edate):
		self.url = "https://archives.nseindia.com/archives/debt/cbm/cbm_trd{}.csv".format(edate.strftime("%Y%m%d"))
		dframe = pd.read_csv(io.StringIO(self.request_url(
			self.url).decode('utf-8')), delimiter=' *, *', engine='python', keep_default_na=False, dtype=str)
		self.main_df = self.main_df.append(dframe,sort=False)
		
	def BseB(self,edate):
		self.url = "https://www.bseindia.com/download/Bhavcopy/Debt/DEBTBHAVCOPY{}.zip".format(edate.strftime("%d%m%Y"))
		dframe = self.UnZipBB(edate)
		self.main_df = self.main_df.append(dframe,sort=False)

	def BseE(self,edate):
		isin = dt.strptime("12312016",'%m%d%Y')
		url = "https://www.bseindia.com/download/BhavCopy/Equity/EQ"
		flNme = "EQ"
		pdate = edate.strftime('%d%m%-y')
		if edate >= isin :
			flNme += "_ISINCODE_" + pdate + ".CSV"
			self.url = url + "_ISINCODE_" + pdate + ".zip"
			dframe = self.Unzip(flNme)
		else :
			flNme += pdate + ".CSV"
			self.url = url + pdate + "_CSV.zip"
			dframe = self.Unzip(flNme)
			#The below columns available when you download the file using ISIN code
			dframe['ISIN_CODE'] = dframe.apply(lambda row: row.SC_CODE , axis = 1)
			
		
		# if 'TRADING_DATE' not in dframe.columns:
		dframe['TRADING_DATE'] = edate.strftime('%d-%b-%Y')
			
		self.main_df = self.main_df.append(dframe,sort=False)
		
		
	def DPrice(self,edate):
		if not edate:
			print("Enter date you would like to download the price.")
			sys.exit(0)
		try:
			if self.callPriFunc == "BE":
				self.BseE(edate)
			elif self.callPriFunc == "BB":
				self.BseB(edate)
			elif self.callPriFunc == "NB":
				self.NseB(edate)
			elif self.callPriFunc == "NF":
				self.NseF(edate)
		except:
			print("Fail to Download {}".format(self.url))
	
	def DHistPrice(self,edate,fdate):
		if not edate and not fdate:
			print("Enter one date you would like to download the price.")
			sys.exit(0)
		
		edate = dt.strptime(edate, '%m%d%Y')
		fdate = dt.strptime(fdate, '%m%d%Y')
		inc = timedelta(1)
		while edate <= fdate :
			self.DPrice(edate)
			edate += inc
# child class


class PpBse(PpSecurity):
	def __init__(self, edate=""):
		self.exchange="bse"
		self.settings=pp_settings()[self.exchange]
		
	def suck_E(self, edate, fdate=""):
		print("Download Prices For Equities")
		self.clrDF()
		self.callPriFunc = 'BE'
		if fdate:
			self.DHistPrice(edate,fdate)
		else:
			edate = dt.strptime(edate, '%m%d%Y')
			self.DPrice(edate)
		
	def suck_B(self,edate,fdate=""):
		print("Download Historical Prices For Bonds")
		self.clrDF()
		self.callPriFunc = 'BB'
		if fdate:
			self.DHistPrice(edate,fdate)
		else:
			edate = dt.strptime(edate, '%m%d%Y')
			self.DPrice(edate)
		
		bondSett = self.settings["saveBy"]["BO"]
		self.settings["ppformat"] = bondSett["ppformat"];
	
	def cpyPri(self,fdate,tdate):
		self.copy_price(fdate,tdate)
		self.copy_price(fdate,tdate,"bond/")

# child class
class PpAmfi(PpSecurity):

	delim=';'			# by default delim is ';'

	# def __init__(self):
	# self.exchange = "amfi"
	# self.settings = ppsettings()[self.exchange]

	def __init__(self, edate="", fdate=""):
		self.exchange="amfi"

		if not edate and not fdate:
			self.settings=pp_settings()[self.exchange]
		else:
			if fdate:
				super().__init__(edate, fdate)
			else:
				super().__init__(edate)

	def suck(self):
		# print("downloading prices from " + self.url + "....")
		self.__soup=bs(self.request_url(self.url).decode('utf-8'), "lxml")
		self.rowsep="\r\n"
		core_path = self.settings['path']['raw'] + 'core/' + dt.now().strftime("%m%d%Y_%H:%M:%S")+".txt"
		with open(core_path,'w+') as fp:
			fp.write(self.__soup.text)

		# self.__soup = bs(self.requesturl("").decode('utf-8'),"lxml")

	def read(self, path):
		try:
			f=open(path, "r")
		except IOError:
			print("Could not read file:", path)
			sys.exit()
		with f:
			self.__soup=bs(f.read(), "lxml")
			self.rowsep="\n"

	def parse(self):

		print("Parsing prices ....")
		self.delim=self.settings["delim"]
		scheme_type=""
		scheme_category=""
		scheme_house=""
		scheme_type_list=["Open", "Close", "Interval"]
		dtable=[]
		if self.__soup.text:

			allLines=self.__soup.text.split(self.rowsep)

			# set column from the source data
			if self.delim in allLines[0]:
				self.columns=[column.strip()
								for column in allLines[0].split(self.delim)]

			# print(self.columns)

			# parse data lines one by one
			for line in allLines[1:]:
				if line.strip():								# check if line is empty
					if self.delim in line:							# checking the line is data row
						# Split data line into a list by delim
						row=line.split(self.delim)
						row.append(scheme_type)
						row.append(scheme_category)
						row.append(scheme_house)
						if len(row) == 10:
							print(line)
						dtable.append(row)
					else:
						# checking the line is Scheme Type and Scheme category
						if '(' in line and ')' in line and any(stype in line for stype in scheme_type_list):
							scheme_type=line.split('(', 1)[0].strip()
							scheme_category=line.split(
								'(', 1)[1].split(')')[0].strip()
						else:
							scheme_house=line

			# Extend columns for 3 extra columns
			self.columns.extend(self.settings["extend"])

			# print(dtable)
			# print(self.columns)
			
			# Make dataframe on parsing mode
			self.main_df=pd.DataFrame(dtable, columns=self.columns)
			dup_rows = []
			self.main_df = self.main_df.rename(columns={"ISIN Div Payout/ISIN Growth": "ISIN"})
			for index,row in self.main_df.iterrows():
				if row['ISIN'].strip() == '-' or row['ISIN'].strip() == "" :
					if row['ISIN Div Reinvestment'].strip():
						row['ISIN'] = row['ISIN Div Reinvestment']
					else:
						row['ISIN'] = row['Scheme Code']
				elif len(row['ISIN Div Reinvestment']) ==  12:
					dupRow = row.copy()
					dupRow['ISIN'] = dupRow['ISIN Div Reinvestment']
					dup_rows.append(dupRow.values)
			
			self.main_df = self.main_df.append(pd.DataFrame(dup_rows, columns=self.main_df.columns))									
			self.main_df['type'] = "mf"
			del dup_rows
		
	def cpyPri(self,fdate,tdate):
		self.copy_price(fdate,tdate)
		
	def DLPrice(self):
		try:
			self.suck()
			self.parse()
			self.save(isCopyRaw=True)
		except:
			self.misspri.append("AMFI fail to download {}".format(self.url))
			# print("Fail to download")

class PpNse(PpSecurity):

	def __init__(self, edate=""):
		self.exchange="nse"
		
		if not edate:
			self.settings=pp_settings()[self.exchange]
		else:
			super().__init__(edate)

	def suck(self):
		self.main_df=pd.read_csv(io.StringIO(self.request_url(
			self.url).decode('utf-8')), delimiter=' *, *', engine='python', keep_default_na=False, dtype=str)
		# self.main_df = pd.read_csv('/home/bharath/Downloads/out.csv', sep=",")
		
	def suck_EQ_Hist(self, edate, fdate):
		print("Download Historical Prices For Equities")
		if not edate and not fdate:
			print("There mush be a From and To Dates for downloading price for the range.")
			sys.exit(0)
			
		edate = dt.strptime(edate, '%m%d%Y')
		fdate = dt.strptime(fdate, '%m%d%Y')
		
		# Downloading a range of Historical Price
		from nsepy.history import get_price_list
		inc = timedelta(1)
		
		dframe = pd.DataFrame();
				
		while edate <= fdate:
			try:
				dframe = get_price_list(dt=edate)
				self.main_df = self.main_df.append(dframe,sort=False)
			except:
				print ("Cannot be download Price file for the date {}.".format(edate))
				self.misspri.append("NSE EQ FRange {}".format(edate))
			edate += inc
		
		eqSett = self.settings["saveBy"]["EQ"]
		self.settings["saveby"] = eqSett["saveby"];
		self.settings["ppformat"] = eqSett["ppformat"];
		# self.main_df=self.main_df.rename(columns = {'CLOSE':'CLOSE_PRICE'})
		
	def suck_B(self,edate,fdate=""):
		print("Download Historical Prices For Bonds")
		if not edate and not fdate:
			print("There mush be an Date for which you wish to downloading price.")
			sys.exit(0)
		
		self.clrDF()
		self.callPriFunc = 'NB'
		if fdate:
			self.DHistPrice(edate,fdate)
		else:
			edate = dt.strptime(edate, '%m%d%Y')
			self.DPrice(edate)
		
		if len(self.main_df.columns) > 2 :
			self.main_df.insert(2,"SERIES","BO")
		else:
			print("File Not Found")
			self.main_df = pd.DataFrame();
				
		bondSett = self.settings["saveBy"]["BO"]
		self.settings["saveby"] = bondSett["saveby"];
		self.settings["ppformat"] = bondSett["ppformat"];
		# self.settings['path'] = bondSett['path']		
		
	def suck_I(self,edate,fdate=""):
		print("Download Historical Prices For Future and Option")
		self.clrDF()
		self.callPriFunc = 'NF'
		if fdate:
			self.DHistPrice(edate,fdate)
		else:
			edate = dt.strptime(edate, '%m%d%Y')
			self.DPrice(edate)
		
		if not self.main_df.empty:
			# self.main_df['SYMB'] = self.main_df.apply(lambda row: row.SYMBOL + dt.strptime(row.EXPIRY_DT,"%d-%b-%Y").strftime("%m%d%Y") + str(row.STRIKE_PR), axis = 1)
			self.main_df['EXPIRY_DT'] = self.main_df.apply(lambda row: dt.strptime(row.EXPIRY_DT,"%d-%b-%Y").strftime("%m%d%Y") , axis = 1)
		
		optSett = self.settings["saveBy"]["OP"]
		self.settings["saveby"] = optSett["saveby"];
		self.settings["ppformat"] = optSett["ppformat"];
		
	def update_holiday(self):

		# read holiday list from central repo
		holidaydf=pd.read_csv(self.settings["holiday-path"], sep="\t")
		holidaydf['Date']=pd.to_datetime(
			holidaydf['Date'], format='%d-%b-%Y').dt.strftime('%m%d%Y')

		# read PP holiday schedule information to get exchange number
		holsc_obj=PpTable("holsched.inf")
		holsc_df=holsc_obj.getdata()
		holsc_df.name=holsc_df.name.str.lower()
		exchange_no=holsc_df.loc[holsc_df['name']
								   == self.exchange].hshed.iloc[0]

		# load Prism Holiday Information
		hol_obj=PpTable("holidayb.inf")
		hol_df=hol_obj.getdata()

		hol_dict={}
		for item in hol_obj.dfidx.fcode.values:
			if item == "hdate":
				hol_dict[item]=holidaydf.Date
			if item == "htype":
				hol_dict[item]=[1 for i in range(len(holidaydf))]
			if item == "hshed":
				hol_dict[item]=[exchange_no for i in range(len(holidaydf))]

		# create new holiday dataframe from central data
		new_holiday=pd.DataFrame(hol_dict)

		hol_df=pd.concat(
			[hol_df.astype(str), new_holiday.astype(str)], ignore_index=True)

		hol_df=hol_df.drop_duplicates()

		hol_df.hdate=pd.to_datetime(hol_df.hdate, format="%m%d%Y")
		hol_df=hol_df.sort_values("hdate")
		hol_df.hdate=hol_df.hdate.dt.strftime("%m%d%Y")

		hol_obj.savedata(hol_df)
		print("data has been updated into {}...".format(hol_obj.tablePath))
		# return hol_df

	def parse(self):

		print("No parsing...")
		
	def cpyPri(self,fdate,tdate):
		self.copy_price(fdate,tdate)
		self.copy_price(fdate,tdate,"bond/")
		self.copy_price(fdate,tdate,"option/")
