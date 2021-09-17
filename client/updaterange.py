#!/usr/bin/env python3

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/..")


from datetime import datetime as dt, timedelta
import pandas as pd
import numpy as np
from libs.ppsettings import pp_settings
from libs.pptable import PpTable
from libs.pppri import pppri


def update_today_price(for_date, date_from="", date_from_nse="", is_for_yesterday_price=True):

    # connect Central Repository prices..
    proobj = pppri()
    proobj.connect(host="local")

    # Get System Currency from Currency Table
    curr_obj = PpTable("currency.inf")
    currdf = curr_obj.get_data()
    curr = currdf.loc[currdf['system'] == 'y'].iloc[0]['curr']

    # Read Security Information Table
    type_obj = PpTable("type.inf")
    typedf = type_obj.get_data().replace(np.nan, '', regex=True)[
        ['type', 'bysl']].drop_duplicates()

    # Read Security Type Information Table
    sec_obj = PpTable("sec.inf")
    secdf = sec_obj.get_data().replace(
        np.nan, '', regex=True)[['type', 'symbol']]

    # merging security with security type
    secdf = pd.merge(secdf, typedf, on='type', how='outer').dropna()

    # -----------------------------------------------------------------------------------------------------
    #                                       For AMFI
    # -----------------------------------------------------------------------------------------------------

    settings = pp_settings()["amfi"]
    stype = settings['type'][''] + curr

    # read prices from centrol repository

    proobj.read(for_date)
    amfi_for_date = proobj.pri_df_raw[proobj.pri_df_raw.symbol.isin(
        secdf[secdf.type == stype].symbol)]

    if date_from != "":
        proobj.read(date_from)
        df_date_from = proobj.pri_df_raw[proobj.pri_df_raw.symbol.isin(
            secdf[secdf.type == stype].symbol)]

        """ 
        -------------------------- Way 1 -----------------------
        In [90]: %timeit out = sat.merge(fri, how='outer', on=['symbol', 'price']).drop_duplicates()
        5.19 ms ± 150 µs per loop (mean ± std. dev. of 7 runs, 100 loops each)

        -------------------------- Way 2 -----------------------
        In [91]: %timeit result_1 = pd.concat([sat,fri],ignore_index=True).drop_duplicates(subset=['symbol'], keep='first')
        1.82 ms ± 26.8 µs per loop (mean ± std. dev. of 7 runs, 1000 loops each)

        -------------------------- Way 3 ( I use this) -----------------------
        In [92]: %timeit result_2 = pd.concat([sat, fri[~fri.symbol.isin(sat.symbol)]], ignore_index=True)
        1.19 ms ± 113 µs per loop (mean ± std. dev. of 7 runs, 1000 loops each)

        -------------------------- Way 4 -----------------------
        In [93]: %timeit out = sat.merge(fri, how='outer', on=['symbol', 'price']).drop_duplicates()
        5.02 ms ± 181 µs per loop (mean ± std. dev. of 7 runs, 100 loops each)
        
        -------------------------- Way 5 (First I tried. It works but slow) -----------------------
        merge_df= pd.merge(df_date_from,amfi_for_date,on='symbol',how="outer",indicator=True)
        merge_df = merge_df[merge_df['_merge']=='left_only']
        merge_df =merge_df.drop(['price_y','_merge'],axis=1)
        merge_df = merge_df.rename(columns = {'price_x':'price'})

        """
        if amfi_for_date.empty:
            amfi_for_date = df_date_from
        else:
            amfi_for_date = pd.concat([amfi_for_date, df_date_from[~df_date_from.symbol.isin(
                amfi_for_date.symbol)]], ignore_index=True)

    amfi_for_date.insert(loc=0, column='type', value=stype)

    # #-----------------------------------------------------------------------------------------------------
    # #                                       For NSE
    # #-----------------------------------------------------------------------------------------------------

    # settings = pp_settings()["nse"]
    # stype = settings['type']

    # # prepare nse type and symbols
    # # get all 'bysl' securities
    # secdf = secdf.loc[secdf.groupby('bysl').groups['y']]

    # # get rid of all 'mf' securities
    # secdf = secdf[secdf.type.str.contains('mf') == 0]

    # # read prices from centrol repository
    # if date_from_nse == "":
    #     date_from_nse = for_date
    # proobj.read(date_from_nse,exchange='nse')

    # # print(proobj.pri_df_raw)

    # # make nse prices groups by type
    # groupnse = proobj.pri_df_raw.groupby('type')

    # pieces = []
    # for index, row in secdf.iterrows():
    #     for type in stype[row['type'][:2]]:

    #         if '@' in type:
    #             pass
    #         else:

    #             if type in groupnse.groups.keys():

    #                 symbolofgroup = groupnse.get_group(type)
    #                 if row['symbol'] in symbolofgroup.symbol.values:
    #                     piece = symbolofgroup[symbolofgroup.symbol == row['symbol']]
    #                     pieces.append([row['type'],row['symbol'],piece.price.iloc[0]])
    #                     break
    #             # print(type)

    # nseprices = pd.DataFrame(pieces,columns=['type','symbol','price'])

    # -----------------------------------------------------------------------------------------------------
    #                                       Update NSE and AMFI price into price table
    # -----------------------------------------------------------------------------------------------------


# update prices into price table
    if is_for_yesterday_price:
        for_date = (dt.strptime(for_date, '%m%d%Y') -
                    timedelta(1)).strftime('%m%d%Y')

    # print(for_date)
    pri_obj = PpTable('%s.pri' % for_date, create_if_not_exist=True)
    pri_obj.save_data(
        pd.concat([amfi_for_date], ignore_index=True), ignore_if_row_exist=True)
    print(pd.concat([amfi_for_date]))

    proobj.close()


def daterange(date1, date2):
    for n in range(int((date2 - date1).days)+1):
        yield date1 + timedelta(n)



if len(sys.argv) != 3:
    print('Enter Start date and End date')
    sys.exit()

fromdate = sys.argv[1]
todate = sys.argv[2]

fromdate = dt.strptime(fromdate, "%m%d%Y")
todate = dt.strptime(todate, "%m%d%Y")

for dt in daterange(fromdate, todate):
    update_today_price(dt.strftime("%m%d%Y"))


# ./updatepri.py "{'date_from':'03222019','for_dates':['03232019']}"


# #
# update_today_price("03212019")
# update_today_price("03222019")
# update_today_price("03232019")
# update_today_price("03242019")
