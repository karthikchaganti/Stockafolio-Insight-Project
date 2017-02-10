
# coding: utf-8

# In[1]:

from faker import *
import sys
import pickle
from faker.providers import BaseProvider
from datetime import datetime,timedelta,time,date
import uuid
import traceback
import subprocess
import csv
import random
import radar
import six
import json
from collections import defaultdict
from pandas import DataFrame
from kafka.client import SimpleClient
from kafka.producer import KeyedProducer
import time as time2
from kafka import KafkaProducer
from kafka.errors import KafkaError
#from boto.s3.connection import S3Connection

# In[2]:

# Definition of the main class
class Simulator():
    # define the constructor
    def __init__(self,dict_stocks_Quandl,ip_addr):

        #self.partition_key = partition_key
        self.ip_addr = ip_addr
        self.producer = KafkaProducer(bootstrap_servers=self.ip_addr)
        #self.userCount = userCount
        self.faker = Faker()
        self.dict_stocks_Quandl = dict_stocks_Quandl
        self.userList_dict = {}
        self.sp500_realtime_dict={}
        #conn = S3Connection('<aws access key>', '<aws secret key>')
#---------------------------------------------------------------------------------------------------#

    def stream_generator(self):

        timestamp = datetime.combine(date.today(),time(10,00,00))
        endtime = time(15,59,59)
        while(True):

            for key in (self.dict_stocks_Quandl):
                ticker_curr_stream = key
                sector_curr_stream = dict_stocks_Quandl[key][0].get('Sector')
                price_stock = float(dict_stocks_Quandl[key][0].get('Price'))
                if(price_stock < 16):
                    rand_price_stock = random.randint(int(price_stock),int(price_stock*1.3))
                else:
                    rand_price_stock = random.gauss(price_stock,(price_stock*0.03))

                self.sp500_realtime_dict[ticker_curr_stream.rstrip()] = {timestamp:rand_price_stock}
                trader_num = random.randint(12000,15000)
            for j in range(trader_num):
                singleTrade = []
                r_n = random.randint(0,999999)
                uuid_trade = self.userList_dict.get(r_n)
                traded_stock,trade_meta_list = random.choice(list(dict_stocks_Quandl.items()))
                traded_stock_sector = trade_meta_list[0].get('Sector')
                traded_quantity = random.randint(150,700)
                traded_stock_price = self.sp500_realtime_dict.get(traded_stock.rstrip()).get(timestamp)
                trade_type = random.choice(['buy','sold'])
                timestamp_str = datetime.strftime(timestamp,"%Y-%m-%d %H:%M:%S")
                #str_fmt = "{};{};{};{}:{};{};{};{}"
                #singleTrade = str_fmt.format(timestamp,uuid_trade,userName_trade,traded_stock,traded_stock_price,traded_quantity,trade_type,traded_stock_sector)
                singleTrade = json.dumps({"timestamp":timestamp_str,"uuid_trade":uuid_trade,"traded_stock":traded_stock.rstrip(),"traded_stock_price":traded_stock_price,
                "traded_quantity":traded_quantity,"trade_type":trade_type,"traded_stock_sector":traded_stock_sector}).encode('utf-8')



                self.producer.send('StreamingTrades',value=singleTrade)
            #time2.sleep(1)
            timestamp += timedelta(seconds=1)
            if(datetime.time(timestamp) > endtime):
                timestamp += timedelta(days=1)
                timestamp = datetime.combine(timestamp,time(10,0,0))
            # time2.sleep(1)
        return None
#---------------------------------------------------------------------------------------------------#
    def userList(self):
        pickler = open('userList.pkl','rb')
        self.userList_dict = pickle.load(pickler)
        #print("Piclkling done!")
        pickler.close()

#---------------------------------------------------------------------------------------------------#
    # main function
if __name__ == "__main__":

    args = sys.argv
    ip_addr = str(args[1])
    #userCount = str(args[3])

    # Initiate the user count
    #userCount = 1000

    # Read the stocks file downloaded from Quandl
    stocks_list_Quandl = open('StocksInfo.csv','r')
    dict_stocks_Quandl = defaultdict(list)
    reader = csv.DictReader(stocks_list_Quandl)
    HEADERS = ["Ticker","Price","Sector"]

    for row in reader:
        key = row.pop('Ticker'.rstrip())
        dict_stocks_Quandl[key].append(row)

    obj_Simulator = Simulator(dict_stocks_Quandl,ip_addr)
    obj_Simulator.userList()
    obj_Simulator.stream_generator()

#----------------------------------End of File---------------------------------------------------------#





# In[ ]:
