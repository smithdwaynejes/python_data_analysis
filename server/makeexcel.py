#!/usr/bin/env python3


import sys, os
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/..")

from libs.ppsecurity import PpAmfi,diff_df
from libs.ppstdlib import list_of_files

def make_excel(ppamfi,file="",sourcedf=None,storein=""):

    storein = file.rsplit('.', 1)[0] if sourcedf is None else storein

    groups = ppamfi.makespreadsheet(sourcedf = sourcedf,groupby='Scheme House',storein =storein )

    if groups:
        with open(storein+".txt","w+") as fobj:
            for group in groups:
                fobj.write(group+'\n')
            print("{} has been created...".format(storein+".txt"))


def run_excel(dirpath):
    ppamfi = PpAmfi()
    # dirpath = '/var/prism/Central/raw/amfi/'
    for file in list_of_files(dirpath,"csv"):
        file = dirpath + file
        ppamfi.read_csv(file)
        make_excel(ppamfi,file)

def run_diff(source,dest,dirpath):
    diffpath = source.rsplit('.',1)[0]
    diffpath += '_' + dest.rsplit('.',1)[0]

    ppamfi_src = PpAmfi()
    ppamfi_dest = PpAmfi()
    
    file = dirpath + source
    print(file)
    ppamfi_src.read_csv(file)
    print(len(ppamfi_src.main_df))

    
    file = dirpath + dest
    print(file)
    ppamfi_dest.read_csv(file)
    print(len(ppamfi_dest.main_df))
    print("------------------------------")
    
    

    make_excel(ppamfi_dest,sourcedf= diff_df(ppamfi_src.main_df,ppamfi_dest.main_df,['Scheme Code']),
                         storein=dirpath+diffpath )


# runexcel('/var/prism/Central/raw/amfi/20190223/')
# runexcel('/var/prism/Central/raw/amfi/20190224/')

# rundiff("02212019_19:51:34.csv", "02212019_19:21:33.csv")
# print("------------------------------------------------")
# rundiff("02212019_20:21:34.csv", "02212019_19:51:34.csv")
# print("------------------------------------------------")
# rundiff("02212019_20:51:33.csv", "02212019_20:21:34.csv")
# print("------------------------------------------------")
# rundiff("02212019_21:21:33.csv", "02212019_20:51:33.csv")
# print("------------------------------------------------")
# rundiff("02212019_21:51:38.csv", "02212019_21:21:33.csv")
# print("------------------------------------------------")
# rundiff("02212019_22:21:33.csv", "02212019_21:51:38.csv")
# print("------------------------------------------------")
# rundiff("02212019_22:51:33.csv", "02212019_22:21:33.csv")
# print("------------------------------------------------")
# rundiff("02212019_23:21:33.csv", "02212019_22:51:33.csv")
# print("------------------------------------------------")
# rundiff('02212019.csv','02212019_23:21:33.csv')
# print("------------------------------------------------")

run_diff('02222019_18:30:10.csv','02222019_18:00:15.csv','/var/prism/Central/raw/amfi/20190223/')
run_diff('02222019_19:00:14.csv','02222019_18:30:10.csv','/var/prism/Central/raw/amfi/20190223/')
run_diff('02222019_19:30:15.csv','02222019_19:00:14.csv','/var/prism/Central/raw/amfi/20190223/')
run_diff('02222019_20:00:16.csv','02222019_19:30:15.csv','/var/prism/Central/raw/amfi/20190223/')
run_diff('02222019_20:30:14.csv','02222019_20:00:16.csv','/var/prism/Central/raw/amfi/20190223/')
run_diff('02222019_21:00:15.csv','02222019_20:30:14.csv','/var/prism/Central/raw/amfi/20190223/')
run_diff('02222019_21:30:16.csv','02222019_21:00:15.csv','/var/prism/Central/raw/amfi/20190223/')
run_diff('02222019_22:00:14.csv','02222019_21:30:16.csv','/var/prism/Central/raw/amfi/20190223/')
run_diff('02222019_22:30:17.csv','02222019_22:00:14.csv','/var/prism/Central/raw/amfi/20190223/')
run_diff('02222019_23:00:16.csv','02222019_22:30:17.csv','/var/prism/Central/raw/amfi/20190223/')

# rundiff('02222019_18:00:12.csv','02222019_18:30:15.csv','/var/prism/Central/raw/amfi/20190224/')
# rundiff('02222019_18:30:15.csv','02222019_19:30:14.csv','/var/prism/Central/raw/amfi/20190224/')
# rundiff('02222019_19:30:14.csv','02222019_20:00:14.csv','/var/prism/Central/raw/amfi/20190224/')
# rundiff('02222019_20:00:14.csv','02222019_20:30:13.csv','/var/prism/Central/raw/amfi/20190224/')
# rundiff('02222019_20:30:13.csv','02222019_21:00:13.csv','/var/prism/Central/raw/amfi/20190224/')
# rundiff('02222019_21:00:13.csv','02222019_21:30:12.csv','/var/prism/Central/raw/amfi/20190224/')
# rundiff('02222019_21:30:12.csv','02222019_22:00:13.csv','/var/prism/Central/raw/amfi/20190224/')
# rundiff('02222019_22:00:13.csv','02222019_22:30:11.csv','/var/prism/Central/raw/amfi/20190224/')
# rundiff('02222019_22:30:11.csv','02222019_23:00:14.csv','/var/prism/Central/raw/amfi/20190224/')
# rundiff('02222019_23:00:14.csv','02222019_23:30:14.csv','/var/prism/Central/raw/amfi/20190224/')


# dirpath = '/var/prism/Central/raw/amfi/'
# print(listofFiles(dirpath,"csv"))