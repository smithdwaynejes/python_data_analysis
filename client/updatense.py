#!/usr/bin/env python3

import sys, os
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/..")

from libs.pppri import pppri
from libs.pptable import PpTable
from libs.ppsettings import pp_settings
import numpy as np
import pandas as pd
from datetime import datetime as dt,timedelta
from libs.ppstdlib import list_of_files

#-----------------------------------------------------------------------------------------------------
#                                       For NSE
#-----------------------------------------------------------------------------------------------------
def update_only_nse(for_date,date_from="",date_from_nse=""):
    # connect Central Repository prices..
    proobj = pppri()
    proobj.connect(host = "local")

    # Get System Currency from Currency Table
    curr_obj = PpTable("currency.inf")
    currdf = curr_obj.get_data()
    curr = currdf.loc[currdf['system'] == 'y'].iloc[0]['curr']


    # Read Security Information Table
    type_obj = PpTable("type.inf")
    typedf = type_obj.get_data().replace(np.nan, '', regex=True)[['type','bysl']].drop_duplicates()

    # Read Security Type Information Table
    sec_obj = PpTable("sec.inf")
    secdf = sec_obj.get_data().replace(np.nan, '', regex=True)[['type','symbol']]

    # merging security with security type
    secdf = pd.merge(secdf,typedf,on='type',how='outer').dropna()




    settings = pp_settings()["nse"]
    stype = settings['type']

    # prepare nse type and symbols
    # get all 'bysl' securities
    secdf = secdf.loc[secdf.groupby('bysl').groups['y']]

    # get rid of all 'mf' securities
    secdf = secdf[secdf.type.str.contains('mf') == 0]

    # read prices from centrol repository
    if date_from_nse == "":
        date_from_nse = for_date
    proobj.read(date_from_nse,exchange='nse')

    # print(proobj.pri_df_raw)

    # make nse prices groups by type
    groupnse = proobj.pri_df_raw.groupby('type')

    pieces = []
    for index, row in secdf.iterrows():
        for type in stype[row['type'][:2]]:

            if '@' in type:
                pass
            else:

                if type in groupnse.groups.keys():

                    symbolofgroup = groupnse.get_group(type)
                    if row['symbol'] in symbolofgroup.symbol.values:
                        piece = symbolofgroup[symbolofgroup.symbol == row['symbol']]
                        pieces.append([row['type'],row['symbol'],piece.price.iloc[0]])
                        break
                # print(type)


    nseprices = pd.DataFrame(pieces,columns=['type','symbol','price'])
    pri_obj = PpTable('%s.pri' %for_date,create_if_not_exist=True)
    pri_obj.save_data(nseprices,ignore_if_row_exist=True)


# Get all csv files to process
list_of_files = list_of_files('/var/prism/Central/pp/nse/', "csv")
# list_of_files = ['03072019.csv']
for item in list_of_files:
    update_only_nse(item.rsplit('.')[0])

# for for_date in ['03052019','03062019','03072019','03082019','03092019','03102019','03112019','03122019','03132019','03142019','03152019','03162019','03172019','03182019']:
#     update_only_nse(for_date)
