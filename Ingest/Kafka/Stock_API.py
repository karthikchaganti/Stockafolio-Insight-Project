
# coding: utf-8

# In[1]:

# The code is used to ingest trade information from Quandl API
# Author: Karthik Chaganti

import datetime
import pandas as pd
from pandas import DataFrame
from pandas.io.data import DataReader
import quandl
#symbols_list = ['AAPL','GOOG']
tickers_file=open('/home/karthik/Desktop/SP.csv','r')
symbols_list = tickers_file.readlines()
#print(symbols_list)
tickers_file.close()
#print (symbols_list)
#sector_file = open('/home/karthik/Projects/Insight/Data_Collect/SP500_Sector.csv','r')
#sector_list = sector_file.readlines()
#sector_file.close()

symbols=[]
quote = '"'
quandl.ApiConfig.api_key = "mQMp_M3zVvm8w9mA13hh"
#try:
    #for symbol,sector in zip(symbols_list,sector_list):
for symbol in symbols_list:
    string = 'WIKI/'+str(symbol).rstrip()
    #print(string)
    data = quandl.get(string,returns = "pandas",start_date="2017-01-18", end_date="2017-01-18",column_index=4)
    sym = symbol.rstrip()
    data['sym'] = symbol
    #data['sector'] = sector
    print (data)
    symbols.append(data)
        
#except: pass
df = pd.concat(symbols)
#define cell with the columns that i need
cell= df[['sym','Close']]
#changing sort of Symbol (ascending) and Date(descending) setting Symbol as first column and changing date format
cell.set_index('sym').to_csv('Stocks_SP500_Final.csv', date_format='%d/%m/%Y')


# In[ ]:


