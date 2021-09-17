#!/usr/bin/env python3
import sys, os
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/..")

from libs.ppsettings import pp_settings
from datetime import datetime as dt, timedelta
import pandas as pd
import numpy as np

def updater2(update_to_df,update_from_df,update_time):
    
    if update_to_df.empty:
        update_to_df[['symbol','price']] = update_from_df[['symbol','price']]
        update_to_df['timelab1'] = update_time
    else:

        # First Get new securities
        merge_df_outer = pd.merge(update_to_df,update_from_df,how='outer',on=['symbol','price'],indicator=True)

        print(merge_df_outer)

        Left_Frame = merge_df_outer[merge_df_outer['_merge'] == 'left_only'].copy()
        Right_Frame = merge_df_outer[merge_df_outer['_merge'] == 'right_only']
        Equal_Frame = merge_df_outer[merge_df_outer['_merge'] == 'both']

        

        s = Right_Frame.set_index('symbol')['price']
        # it mapping column Left_Frame['symbol'] by Series - s , similar like dictionary. 
        # If not matched,get NaN, so this values are replaced by original column
        Left_Frame['price'] = Left_Frame['symbol'].map(s).fillna(Left_Frame['price'])
        
        Left_Frame['timelab3'] = np.where(Left_Frame['timelab3'].values != '',update_time,'')

        Left_Frame['timelab3'] = np.where(Left_Frame['timelab2'].values != '',update_time,'')

        Left_Frame['timelab2'] = np.where(Left_Frame['timelab2'].values == '',update_time,Left_Frame['timelab2'].values)
        
        Right_Frame = Right_Frame[~Right_Frame.symbol.isin(Left_Frame.symbol)]

        Right_Frame['timelab1'] = update_time

        # print(len(Left_Frame))
        # print(len(Right_Frame))
        # print(len(Equal_Frame))

        Equal_Frame = Equal_Frame.drop('_merge',axis=1)
        Right_Frame = Right_Frame.drop('_merge',axis=1)
        Left_Frame = Left_Frame.drop('_merge',axis=1)

        update_to_df = pd.concat([Equal_Frame,Right_Frame,Left_Frame],ignore_index=True).drop_duplicates(subset=['symbol'], keep='last')

        # print(update_to_df)
        # s = Right_Frame.set_index('symbol')['price']
        # Left_Frame['timelab2'] = update_time
        # print (Left_Frame)

        # Left_Frame['price1'] = Left_Frame['symbol'].map(s)
        # print(Left_Frame)

        # Left_Frame['price2'] = Left_Frame['symbol'].map(s).fillna(Left_Frame['price'])
        # print(Left_Frame)

       

    return update_to_df.where((pd.notnull(update_to_df)), '')


def updater(update_to_df,update_from_df,update_time):
    
    if update_to_df.empty:
        update_to_df[['symbol','price']] = update_from_df[['symbol','price']]
        update_to_df['timelab1'] = update_time
    else:
        # First Get new securities
        merge_df_outer = pd.merge(update_to_df,update_from_df,how='outer',indicator=True)

        equal_df = merge_df_outer[merge_df_outer['_merge'] == 'both']
        only_new_df = merge_df_outer[merge_df_outer['_merge'] == 'right_only']
        updated_old_price_df = merge_df_outer[merge_df_outer['_merge'] == 'left_only']

        update_new_df = only_new_df[only_new_df.symbol.isin(updated_old_price_df.symbol)]
        
        only_new_df ['timelab1'] = update_time

        updated_old_price_df['price'] = np.where(updated_old_price_df['symbol'].values == update_new_df['symbol'].values, update_new_df['price'],updated_old_price_df['price'])
        # left_frame['price'] = np.where(left_frame['symbol'].values == right_frame['symbol'].values, right_frame['price'],left_frame['price'])
        # updated_old_price_df['timelab2'] = update_time
        updated_old_price_df['timelab2'] = np.where(updated_old_price_df['timelab2'].values == '', update_time, '')
       
        equal_df = equal_df.drop('_merge',axis=1)
        only_new_df = only_new_df.drop('_merge',axis=1)
        update_new_df = update_new_df.drop('_merge',axis=1)
        update_to_df = pd.concat([equal_df,only_new_df,updated_old_price_df],ignore_index=True).drop_duplicates(subset=['symbol'], keep='last')
        update_to_df = update_to_df.drop('_merge',axis=1)


    return update_to_df.where((pd.notnull(update_to_df)), '')


def load_update_table(path,update_to_columns=[]):
    update_to_df = pd.DataFrame(columns = update_to_columns)
    if os.path.exists(path):
        update_to_df = pd.read_csv(path, delimiter=' *, *', engine='python',
								keep_default_na=False, dtype=str)
    return update_to_df

def make_update_to_path(path,date):
    return path+date+".csv"

def save_update_to_df(update_to_path,update_to_df):
    update_to_df.to_csv(update_to_path, mode='w+', index=False, header=True)

if len(sys.argv) != 3:
    print('Enter path of update from and date to update')
    sys.exit()

update_from_path = sys.argv[1]
date_for_update = sys.argv[2]
update_time = update_from_path.split('_')[1][:5]

settings = pp_settings()

update_to_path = settings['dailypri']['path']
update_to_columns = settings['dailypri']['format']

update_from_path = settings['amfi']['path']['pp'] + update_from_path

update_to_path = make_update_to_path(update_to_path,date_for_update)

update_to_df = load_update_table(update_to_path,update_to_columns)
update_from_df = load_update_table(update_from_path)


update_to_df = updater2(update_to_df, update_from_df, update_time)

update_to_df = update_to_df[update_to_columns]
save_update_to_df(update_to_path,update_to_df)

# print(update_from_df)
print(update_to_df)
