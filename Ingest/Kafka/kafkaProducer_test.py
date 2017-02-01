#! /usr/bin/python

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
import threading
from collections import defaultdict
from pandas import DataFrame
from kafka.client import SimpleClient
from kafka.producer import KeyedProducer

from kafka import KafkaProducer
from kafka.errors import KafkaError
import json

# In[2]:

# Definition of the main class
class Simulator():
    # define the constructor
    def __init__(self,userCount,stream_Interval,dict_stocks_Quandl):


        #self.cnctnAddr = cnctnAddr
        #self.producer = KafkaProducer(bootstrap_servers=self.cnctnAddr)

        #self.cleanFiles()
        self.userList_dict = {}
        self.userCount = userCount
        self.stream_Interval = stream_Interval
        self.faker = Faker()
        self.dict_stocks_Quandl = dict_stocks_Quandl
        self.sp500_realtime_dict={}

        self.btchStrtTime = '2017-01-09T10:00:00'
        self.btchEndTime = '2017-01-13T15:59:59'
        self.streamTime = '2017-01-10 09:48:49'

        self.userDatabaseDir = "/home/karthik/Projects/Insight/Data_Collect/output/"
        self.userDatabaseName = "userDB.csv"
        self.userDatabaseHeader = ['userID', 'userName','Ticker','Price','Sector','Quantity','Date']
        self.userDatabasePath = (self.userDatabaseDir+self.userDatabaseName)
#######################------------------------------------------------------#########################

    # Create users
    def userNameGenerator(self):
        #for i in range(self.userCount):
        user_ID = self.gen_UniqueId() # random
        user_Name = self.faker.name() # random

        rand_stock_count = random.randint(1,15)
           # print ("stock_count:" + str(rand_stock_count))
        for r in range(rand_stock_count):
            currentRow = []
            stock_ticker,stock_meta = random.choice(list(self.dict_stocks_Quandl.items()))
            stock_price = stock_meta[0].get('Price')
            stock_sector = stock_meta[0].get('Sector')
            rand_stock_quantity = random.randint(100,400)
            rand_timestamp = radar.random_datetime(start=self.btchStrtTime, stop=self.btchEndTime)
            rand_Date = rand_timestamp.strftime("%Y-%m-%d %H:%M:%S")

            #print("quant:"+ str(rand_stock_quantity))
            currentRow.append(user_ID)
            currentRow.append(user_Name)
            currentRow.append(stock_ticker)
            currentRow.append(stock_price)
            currentRow.append(stock_sector)
            currentRow.append(rand_stock_quantity)
            currentRow.append(rand_Date)
            #print(currentRow)
            self.writeFile(currentRow)
        return None

#######################------------------------------------------------------#########################

    # Method to write the data to a certain file
    def writeFile(self,data):
        # Try writing the file
        #filepath = "./home/karthik/Projects/Insight/Data_Collect/output/userDatabase/userDB.csv"
        try:
            # open and write the data list
            with open("/home/karthik/Projects/Insight/Data_Collect/output/x.csv",'a+') as filer:
                # write down the data passed on to it
                csvFileWrtr = csv.writer(filer)
                csvFileWrtr.writerow(data)
            return True

        # Exception
        except Exception:
            traceback.print_exc()
            return False
#######################------------------------------------------------------#########################

# Method to write the data to a certain file
    def writeStream(self,data):
        # Try writing the file
        #filepath = "./home/karthik/Projects/Insight/Data_Collect/output/userDatabase/userDB.csv"
        try:
            # open and write the data list
            with open("/home/karthik/Projects/Insight/Data_Collect/output/streamX.csv",'a+') as fileWr:
                # write down the data passed on to it
                csvFileWrtr = csv.writer(fileWr)
                csvFileWrtr.writerow(data)
            return True

        # Exception
        except Exception:
            traceback.print_exc()
            return False
#######################------------------------------------------------------#########################
    # Entry to the simulation
    def user_generatorFunc(self):
        self.writeFile(self.userDatabaseHeader)
        for i in range(self.userCount):
            self.userNameGenerator()
#######################------------------------------------------------------#########################
    def stream_generator(self):
        timestamp = datetime.now()
        for i in range(30):
            singleTrade = []
            for key in (self.dict_stocks_Quandl):
                ticker_curr_stream = key
                sector_curr_stream = dict_stocks_Quandl[key][0].get('Sector')
                price_stock = float(dict_stocks_Quandl[key][0].get('Price'))
                if(price_stock < 16):
                    rand_price_stock = random.randint(int(price_stock),int(price_stock*1.3))
                else:
                    rand_price_stock = random.gauss(price_stock,(price_stock*0.03))
                #print(rand_price_stock)
                self.sp500_realtime_dict[ticker_curr_stream.rstrip()] = {timestamp:rand_price_stock}
            #print(self.sp500_realtime_dict)

            uuid_trade,userName_trade = random.choice(list(self.userList_dict.items()))
            traded_stock,trade_meta_list = random.choice(list(dict_stocks_Quandl.items()))
            traded_stock_sector = trade_meta_list[0].get('Sector')
            traded_quantity = random.randint(5,150)
            traded_stock = str(traded_stock.rstrip())
            #timestamp1 = str(timestamp)
            traded_stock_price = self.sp500_realtime_dict.get(traded_stock).get(timestamp)
            print(traded_stock_price)
            trade_type = random.choice(['buy','sold'])
            singleTrade.append(uuid_trade)
            singleTrade.append(userName_trade)
            singleTrade.append(traded_stock)
            singleTrade.append(traded_stock_sector)
            singleTrade.append(traded_quantity)
            singleTrade.append(traded_stock_price)
            singleTrade.append(trade_type)
            timestamp_str = datetime.strftime(timestamp,"%Y-%m-%d %H:%M:%S")
            singleTrade.append(timestamp_str)
            #singleTrade = json.dumps({"timestamp":timestamp_str,"uuid_trade":uuid_trade,"traded_stock":traded_stock,"traded_stock_price":traded_stock_price,
            #"traded_quantity":traded_quantity,"trade_type":trade_type,"traded_stock_sector":traded_stock_sector})
            #str_fmt = "{};{};{};{};{};{};{};{}"
            #singleTrade = str_fmt.format(timestamp,uuid_trade,userName_trade,traded_stock,traded_stock_price,traded_quantity,trade_type,traded_stock_sector)
            self.writeStream(singleTrade)
            #self.producer.send('StockStream',key = self.partition_key,value=singleTrade)
            timestamp += timedelta(seconds=1)
        return None
        #######################------------------------------------------------------#########################
    def stream_trades_generator(self):
        time_str = self.streamTime
        timestamp_trade = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
        while(i in range(100)):
            trade = []
            uuid_trade,userName_trade = random.choice(list(self.userList_dict.items()))
            traded_stock,x,y = random.choice(list(dict_stocks_Quandl.items()))

            traded_stock_price = self.sp500_realtime_dict[timestamp_trade][traded_stock]
            trade_type = random.choice('buy','sold')
            trade = append(uuid_trade)
            trade = append(userName_trade)
            trade = append(traded_stock)
            trade = append(traded_stock_price)
            trade = append(trade_type)
            trade = append(timestamp)
            self.writeStream(trade)
            #self.producer.send('TradeStream',str(trade))  #producer.send(topic,message)
            timestamp += timedelta(seconds=1)
        return None

#######################------------------------------------------------------#########################
    def gen_UniqueId(self):
        # Use uuid4 and return an unique id
        return str(uuid.uuid4())

    def cleanFiles(self):
        # Define the command
       # thisCmd = 'rm -rf '+str('/home/karthik/Projects/Insight/Data_Collect/output/userDatabase/x.csv')
        # Run the command
        self.runBash(thisCmd)
    def runBash(self,cmd):
        # Run a cmd and return exceptions if any
        try:
            subprocess.call(cmd,shell=True)
        except Exception:Producer

    #def kakfaProducer(self,topic,msg):
        #self.producer.send(self.topic,self.msg)
    def userList(self):
        for i in range(30):
            user_ID = str(uuid.uuid4()) # random
            user_Name =Faker().name()
            self.userList_dict[user_ID] = user_Name

#######################------------------------------------------------------#########################
    # main functionsp500_realtime_dict
if __name__ == "__main__":
    # Total no.of. Users
    # Change this value to increase the no.of. users
    userCount = 30
    stream_Interval = 0 # use Zero for Batch Process
    # Read the stocks file downloaded from Quandl
    stocks_list_Quandl = open('/home/karthik/Projects/Insight/Data_Collect/stoc.csv','r')
    dict_stocks_Quandl = defaultdict(list)
    reader = csv.DictReader(stocks_list_Quandl)
    HEADERS = ["Ticker","Price","Sector"]

    for row in reader:
        key = row.pop('Ticker'.rstrip())
        dict_stocks_Quandl[key].append(row)
    #print(dict_stocks_Quandl)
    obj_Simulator = Simulator(userCount,stream_Interval,dict_stocks_Quandl)
    #obj_Simulator.stream_generator()
    #obj_Simulator = Simulator(userCount,stream_Interval,dict_stocks_Quandl,ip_addr)
    #obj_Simulator.user_generatorFunc()
    #t = threading.Thread(name = 'usersList',target=obj_Simulator.userList())
    #t1 = threading.Thread(name = 'stream_stocks',target=obj_Simulator.stream_generator())
    #t2 = threading.Thread(name='stream_trades',target=obj_Simulator.stream_trades_generator())
    obj_Simulator.userList()
    obj_Simulator.stream_generator()
    #obj_Simulator.stream_trades_generator()

#######################------------------------------------------------------#########################





# In[ ]:
