#!/usr/bin/env python3

import sys, os

from libs.ppreadssh import param_ssh
from libs.ppsettings import pp_settings

import pandas as pd
class pppri:
    exchange = ""
    misspri = []
    pri_df_raw = pd.DataFrame()
    def __init__(self):
        self.settings = pp_settings()

    def connect(self,host = "central"):

        # Connect with Web Repository
        print("Connecting Repo...")
        ssh_client = param_ssh(self.settings[host+'-host'],self.settings[host+'-user'],self.settings[host+'-pass'])
        self.sftp_client = ssh_client.open_sftp()
        print("Connected Repo Successfully...")
    
    def close(self):
        print("Closing Connection...")
        self.sftp_client.close()
    
    def show(self):
        print(self.price)
        print(self.pri_df_raw)

    def read(self,price="",byPP = True,exchange="amfi",intoDir=''):
        self.pri_df_raw = pd.DataFrame()
        getby = "pp"
        if not byPP:
            getby = "raw"
        if not price:
            print("Price name should not be empty")
            sys.exit()

        self.exchange = exchange
        if intoDir != '':
            intoDir += '/'
        self.price = pp_settings()[self.exchange]['path'][getby]+intoDir+price+".csv"

        try:
            self.sftp_client.stat(self.price)
            # print(self.price + ' exists ...')
            # sourcedf = pd.read_csv(.rename(columns=lambda x: x.strip()).groupby('SYMBOL')
            self.pri_df_raw = pd.read_csv(self.sftp_client.open(self.price), dtype=str, keep_default_na=False)
            
        except IOError:
            # print("File Missing {}".format(self.price))
            self.misspri.append(self.price)
            # sys.exit()

