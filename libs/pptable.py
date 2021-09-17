import pandas as pd
import time
import numpy as np
import sys
import os
from libs.exceptions import CopyError


try:
    from itertools import izip_longest  # added in Py 2.6
except ImportError:
    from itertools import zip_longest as izip_longest  # name change in Py 3.x

try:
    from itertools import accumulate  # added in Py 3.2
except ImportError:
    def accumulate(iterable):
        'Return running totals (simplified version).'
        total = next(iterable)
        yield total
        for value in iterable:
            total += value
            yield total

def make_parser(fieldwidths):
    cuts = tuple(cut for cut in accumulate(abs(fw) for fw in fieldwidths))
    pads = tuple(fw < 0 for fw in fieldwidths) # bool values for padding fields
    flds = tuple(izip_longest(pads, (0,)+cuts, cuts))[:-1]  # ignore final one
    parse = lambda line: tuple(line[i:j] for pad, i, j in flds if not pad)
    # optional informational function attributes
    parse.size = sum(abs(fw) for fw in fieldwidths)
    parse.fmtstring = ' '.join('{}{}'.format(abs(fw), 'x' if fw < 0 else 's')
                                                for fw in fieldwidths)
    return parse

def load_files(filename,delim):
	try:
		with open(filename) as file_object:
			configdict ={}
			for line in file_object.read().splitlines():
				a,b = line.split(delim,1) 
				configdict[a]=b
		return configdict
	except IOError:
		print ("Error: can\'t find file or read data on " + filename)

class PpTable:
	modes = ["r","w","a"]
	def __init__(self, Tablename, mode="r",create_if_not_exist=False):
		if mode not in self.modes:
			print("Invalid mode option!", self.modes)
			sys.exit()
			 
		self.mode = mode
		self.Tablename = Tablename
		
		# Get Table Name and Extension from Tablename
		self.thead,self.ttail = Tablename.split('.')
		
		# loading PP data configuration
		self.dir_path = load_files("/var/prism/config/.prism.conf",":")['DATADIR']
		
		# loading Leveltype Informations
		levelType = load_files(self.dir_path+"App/Indexes/leveltype.idx",'\t')[self.ttail] + "/" + self.ttail + "/"
		
		# set Table Path
		self.tablePath = self.dir_path + levelType + self.Tablename
		
		if not os.path.exists(self.tablePath):
			if create_if_not_exist:
				self.file_object = open(self.tablePath,"w")
			else:
				raise CopyError('701', self.tablePath)

		if mode == "w":
			if os.path.exists(self.tablePath):
				 mode = "a"
			else:
				mode = "w+"
		self.file_object = open(self.tablePath,mode)
		
		self.indexPath = load_files(self.dir_path+"App/Indexes/indexnames.idx",'\t')[self.ttail]
		if self.indexPath == "s":
			self.indexPath = self.thead

		# loading Index Path Information
		self.indexPath = self.dir_path + "App/Indexes/idx/"+ self.indexPath +".idx"
		
		# loading index File
		self.dfidx = pd.read_csv(self.indexPath,sep="\t",
							names=['fcode','fdesc','fstartidx','fsize','ftype','precision','fcons','notnull','ronly','wdth','res2','cel','flr'],encoding = 'utf8')

		self.tabledf = pd.read_fwf(self.tablePath, widths=self.dfidx.fsize.values,names=self.dfidx.fcode.values,dtype=str,
										keep_default_na=False)
	
	def get_data(self):
		return self.tabledf

	def save_data(self,tdata,mode="a",ignore_if_row_exist=False,on=''):
		
		self.tformat = "".join("%-"+str(i)+"s" for i in self.dfidx.fsize.values)
		if ignore_if_row_exist:
			if on is '':
				on = tuple(self.dfidx.fcode.values)

			res_df=pd.merge(tdata,self.tabledf,on=on,how="outer",indicator=True)
			res_df=res_df[res_df['_merge']=='left_only']
			res_df =res_df.drop('_merge',axis=1)
		else:
			res_df = tdata
		
		# print("result Data Frame ")
		# print(res_df)
		if not res_df.empty:
			with open(self.tablePath,mode) as ofile:
				np.savetxt(ofile, res_df.values, fmt=self.tformat)
				print('Data has been saved into {}'.format(self.tablePath))


