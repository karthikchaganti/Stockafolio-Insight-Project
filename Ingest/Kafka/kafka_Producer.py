
# coding: utf-8

# In[1]:

from faker import *
import sys
from faker.providers import BaseProvider
from datetime import datetime,timedelta
import time
import uuid
import traceback
import subprocess
import csv
import random
import radar
import six
import JSON
from collections import defaultdict
from pandas import DataFrame
from kafka.client import SimpleClient
from kafka.producer import KeyedProducer

from kafka import KafkaProducer
from kafka.errors import KafkaError

# In[2]:

# Definition of the main class
class Simulator():
    # define the constructor
    def __init__(self,userCount,dict_stocks_Quandl,ip_addr,partition_key):

        self.partition_key = partition_key
        self.ip_addr = ip_addr
        self.producer = KafkaProducer(bootstrap_servers=self.ip_addr)
        self.userCount = userCount
        self.faker = Faker()
        self.dict_stocks_Quandl = dict_stocks_Quandl
        self.userList_dict = {}
        self.sp500_realtime_dict={}
#---------------------------------------------------------------------------------------------------#

    def stream_generator(self):
        #timestamp = datetime.strptime(self.streamTime, "%Y-%m-%d %H:%M:%S")
        timestamp = datetime.now()
        while(True):
            singleTrade = []
            for key in (self.dict_stocks_Quandl):
                ticker_curr_stream = key
                sector_curr_stream = dict_stocks_Quandl[key][0].get('Sector')
                price_stock = float(dict_stocks_Quandl[key][0].get('Price'))
                if(price_stock < 16):
                    rand_price_stock = random.randint(int(price_stock),int(price_stock*1.3))
                else:
                    rand_price_stock = random.gauss(price_stock,(price_stock*0.03))

                self.sp500_realtime_dict[ticker_curr_stream.rstrip()] = {timestamp:rand_price_stock}

            uuid_trade,userName_trade = random.choice(list(self.userList_dict.items()))
            traded_stock,trade_meta_list = random.choice(list(dict_stocks_Quandl.items()))
            traded_stock_sector = trade_meta_list[0].get('Sector')
            traded_quantity = random.randint(5,150)
            traded_stock_price = self.sp500_realtime_dict.get(traded_stock.rstrip()).get(timestamp)
            trade_type = random.choice(['buy','sold'])
            timestamp_str = datetime.strftime(timestamp,"%Y-%m-%d %H:%M:%S")
            #str_fmt = "{};{};{};{}:{};{};{};{}"
            #singleTrade = str_fmt.format(timestamp,uuid_trade,userName_trade,traded_stock,traded_stock_price,traded_quantity,trade_type,traded_stock_sector)
            singleTrade = json.dumps({"timestamp":timestamp_str,"uuid_trade":uuid_trade,"traded_stock":traded_stock,"traded_stock_price":traded_stock_price,
            "traded_quantity":traded_quantity,"trade_type":trade_type,"traded_stock_sector":traded_stock_sector})

            self.producer.send('TradesTopic',key = self.partition_key,value=singleTrade)
            timestamp += timedelta(seconds=1)
        return None
#---------------------------------------------------------------------------------------------------#
    def gen_UniqueId(self):
        # Use uuid4 and return an unique id
        return str(uuid.uuid4())

    def userList(self):
        for i in range(userCount):
            user_ID = str(uuid.uuid4()) # random
            user_Name =self.faker.name()
            self.userList_dict[user_ID] = user_Name

#---------------------------------------------------------------------------------------------------#
    # main function
if __name__ == "__main__":

    args = sys.argv
    ip_addr = str(args[1])
    partition_key = str(args[2])
    userCount = str(args[3])

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

    obj_Simulator = Simulator(userCount,dict_stocks_Quandl,ip_addr,partition_key)
    obj_Simulator.userList()
    obj_Simulator.stream_generator()

#----------------------------------End of File---------------------------------------------------------#





# In[ ]:
