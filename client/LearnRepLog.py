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

import urllib.parse
import re

def parseDataSet(rundf):
	L = []
	for v in rundf['Query']:
		out = {}
		for x in v.split('&'):
			a, b = x.split('=')
			out[re.sub('[#$%]', '',urllib.parse.unquote(a))] = urllib.parse.unquote(b)
		L.append(out)

	# print (L)
	# [{'$name': 'XXX', '#age': '18', '#mark': '100'}, 
	  # {'$name': 'YYY', '#age': '17', '#mark': '95'}]

	rundf = rundf.join(pd.DataFrame(L))
	return(rundf)
	
	
if len (sys.argv) != 7 :
    print ("Usage: User, Date, Columns(Sep by Space), Groupby, Group Val, Want to Save Pdf[y/n] ")
    sys.exit (1)


arguments = sys.argv[1:]

user = arguments[0]
date = arguments[1]



groupby = []
GroupVal = []
columns = []
downloadpdf = "n"
if arguments[2]:
	columns = arguments[2].split(' ')
if arguments[3]:
	groupby = arguments[3].split(' ')
if arguments[4]:
	GroupVal = arguments[4].split(' ')
if arguments[5]:
	downloadpdf = arguments[5]


# loading Prims Portfolio Settings
print("loading PP Settings...")
settings = pp_settings()


# Set Web Repository connections
address=settings['web-host']
username=settings['web-user']
password=settings['web-pass']


# Connect with Web Repository
print("Connecting Web Repo...")
ssh_client = param_ssh(address,username,password)
sftp_client = ssh_client.open_sftp()
print("Connected Web Repo Successfully...")


sourcepath =  settings['logpath'] + user + "/log/"+date+"/replog.log"

try:
	sftp_client.stat(sourcepath)
	print(sourcepath + ' exists ...')
	
	sourcedf = pd.read_csv(sftp_client.open(sourcepath),sep="\t",names=["Time","Success","Session","Report","Mode","Query"],encoding = 'utf8')
	
	soldf = parseDataSet(sourcedf)
	soldf.Time = pd.to_datetime(soldf.Time,unit='ms')
	soldf.Time = soldf.Time.apply(lambda x: x.strftime("%I:%M:%S"))

	print("Available columns are .... ")
	print(soldf.columns.values)

	soldf = soldf.replace(np.nan, '', regex=True)

	setApplied = soldf

	# --------------------- Dont delete -----------------
	# df.item = df.item.apply(lambda x: ' '.join(x.split()[0].split('~')[0]))
	# Or:

	# df.item = df.item.str.split().str[0].str.split('~').str[0].str.join(' ')

	# df.item = [ ' '.join(x.split()[0].split('~')[0]) for x in df.item]


	# --------------------- Dont delete -----------------


	# print(soldf['askport'])
	askport = []
	for item in soldf.askport:
		if item:
			# print(soldf.askport[item])
			myports = ""
			# print(soldf.askport[item])
			for ports in item.split(' '):
				myports = myports + " " +ports.split('~')[0]
			askport.append(myports)
		else:
			askport.append("")
				# myports = ""
				# for port in ports.split('~'):
					# print(port)
					# myports = myports + " " +port
				# print(myports)
	# soldf.askport  = soldf.askport .str.split(r'\W').apply(lambda x: ' '.join(x[::1]))

	soldf.askport = askport

	isGroupby = False
	if len(groupby) != 0:
		if  pd.Series(groupby).isin(setApplied.columns).all():
			setApplied = setApplied.groupby(groupby)
			isGroupby = True
			if len(GroupVal) != 0:
				if setApplied.groups:
					isGroupby = False
					print(len(GroupVal))
					if len(GroupVal) == 1:
						setApplied = setApplied.get_group(GroupVal[0])
					else:
						setApplied = setApplied.get_group(tuple(GroupVal))


	pdfList = []

	if isGroupby:
		for grp,val in setApplied:
			print(grp)
			if columns:
				print(val[columns])
				
			else:
				print(val)
			if downloadpdf == "y":
				pdfList = val['pdfname']
	else:
		if columns:
			print(setApplied[columns])
		else:
			print(setApplied)
		if downloadpdf == "y":
				pdfList = setApplied['pdfname']
			
	try:
		if downloadpdf == "y":
			getlist =   [ i for i in pdfList if i]

			for pdf in getlist:
				sftp_client.get('/var/prism/data/Reports/doc_view/'+pdf+'.pdf','exp/'+pdf+'.pdf')
			
			print("{} pdf's are updated to exp folder".format(len(getlist)))
	
	except IOError:
		print("Could not export ..")
		
	
except IOError:
	
	print("File not found {} ..".format(sourcepath))
	

# close connection Web Repository
ssh_client.close()
sftp_client.close()
