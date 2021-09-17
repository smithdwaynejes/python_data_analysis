## Need packages to run Interface

sudo apt install python3-pip

pip3 install pandas

pip3 install paramiko

sudo apt-get install python3-bs4

pip3 install cryptography==2.4.2

pip install nsepy


## In nsepy is not having Equity download url.
# Please do the following steps.

In url.py (located in ~/.local/lib/python(version-number)/sites-packages/nsepy) change the price_list_url replaced by this "price_list_url = URLFetchSession(url='https://archives.nseindia.com/content/historical/EQUITIES/%s/%s/cm%sbhav.csv.zip')" at line no:61

