# Spark Streaming Coded
# Spark Version : 2.1.0 (Hadoop 2.7)
# groupId : org.apache.Spark
# artifactId : spark-streaming-kafka-0-10_2.11
# Kafka Version : 0.10.1.1
# Kafka Spark Connector :

from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cqlengine import connection
from cqlengine import columns
from cqlengine.models import Model
from cqlengine.management import sync_table
from datetime import datetime
import json
import uuid


class Streamer():

    def __init__(self):


    def getSqlContextInstance(sparkContext):
        if ('sqlContextSingletonInstance' not in globals()):
            globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
        return globals()['sqlContextSingletonInstance']

    def process(rdd):
        sqlContext = getSqlContextInstance(rdd.context)
        rowRdd = rdd.map(lambda w: Row(uuid_trade=str(json.loads(w)["uuid_trade"]), userName_trade=json.loads(w)["userName_trade"],
        timestamp=json.loads(w)["timestamp"] ,traded_stock=json.loads(w)["traded_stock"],traded_stock_price=json.loads(w)["traded_stock_price"],traded_quantity=json.loads(w)["traded_quantity"],
        trade_type=json.loads(w)["trade_type"],traded_stock_sector=json.loads(w)["traded_stock_sector"]))
        df_trades = sqlContext.createDataFrame(rowRdd)
        print(df_trades)
        for row in df_trades.collect():
            stsp_timestamp  = row.timestamp.encode('utf-8')
            tradeTime  = datetime.strptime(stsp_timestamp, "%Y-%m-%d %H:%M:%S")
            userName = row.userName_trade
            userId = row.uuid_trade
    	    userId = uuid.UUID(userId)
            tradeType = row.trade_type
            tickerName = row.traded_stock
            tickerSector = row.traded_stock_sector
            tickerPrice = row.traded_stock_price
            tradeQuantity = row.traded_quantity
            if tickerPrice != None:
               total_val = tickerPrice * tradeQuantity
            else:
               total_val = 0
            # Push the trade to the trade history database
            session.execute(db_pushTrade,(userId,userName,tickerName,tickerSector,tickerPrice,tradeQuantity,total_val,tradeTime,tradeType))

            rts1_stocks = session.execute(db_portfolio_values, (userId,tickerName, ))
            rts1_stock = 0 if len(rts1_stocks) == 0 else rts1_stocks[0].tickerQuant


            ##########################################################################################
            ##########################################################################################
    def db_getValue(table):
        value = "SELECT tickerQuant, tickerValue FROM " + table + "WHERE userId = ? AND tickerName = ?"
        ses_val = session.prepare(value)
        ses_val.consistency_level = ConsistencyLevel.LOCAL_QUORUM
        return ses_val

    def db_getCount(table):
        count = "SELECT portfolio_count, portfolio_value FROM" + table + "WHERE userId = ?"
        ses_count = session.prepare(count)
        ses_count.consistency_level = ConsistencyLevel.LOCAL_QUORUM
        return ses_count

    def calculate_portfolio_total(company_stock, total_stock, tradeamount):
        if company_stock > 0:
            if tradeamount >= 0:
                #if holding a postive amount and buying more, just increase total portfolio
                return total_stock + tradeamount
            else:
                #if selling, first see if the amount being held is still postive or negative
                new_stock = company_stock + tradeamount
                if new_stock >= 0:
                    #if it is still positive, subtract the amount from the total portfolio (+ because negative number)
                    return total_stock + tradeamount
                else:
                    #if it becomes negative, subtract all holdings, then add the remaining negative shares as a short
                    sell = -tradeamount
                    return total_stock - company_stock + (sell - company_stock)
        if company_stock < 0:
            #treat a negative count as a short, which makes a positive count towards portfolio size for news relevance
            if tradeamount < 0:
                #selling more on a short counts as increasing portfolio
                return total_stock + abs(tradeamount)
            else:
                #see if the amount being held becomes positive or negative
                new_stock = company_stock + tradeamount
                if new_stock <= 0:
                    #still negative, which means less shares are shorted, decrease the amount from total portfolio
                    return total_stock - abs(tradeamount)
                else:
                    #if it becomes positive, subtract all holdings, then add the remaining positive shares as a buy
                    return total_stock - abs(company_stock) + (company_stock + tradeamount)
        else:
            #previously held 0 shares, add the amount being traded to portfolio total
            return total_stock + abs(tradeamount)
            #############################################################################
            #############################################################################
    #def db_getSectorProp(table):
        #proportion = "SELECT sec_prop FROM" + table + "WHERE userId = ? AND tickerSector = ?"
        #ses_prop = session.prepare(proportion)
        #return ses_prop

if __name__ == "__main__":
    # Spark Context Config
    conf = SparkConf().setAppName("StockAFolio").set("spark.cores.max", "12")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 1) # Window 1 seconds

    zkQuorum = "localhost:2181"
    kafka_topic = "NewTopic"
    kafka_brokers = "ec2-34-198-10-253.compute-1.amazonaws.com:9092"
    # Connect to Cassandra
    server_EC2 = Cluster(['ec2-34-198-236-106.compute-1.amazonaws.com'])
    session = server_EC2.connect('stockportfolio')
    # prepares the session for pushing the latest trades into the database
    db_portfolio_values = self.db_getValue("db_user_portfolio")
    db_portfolio_count = self.db_getCount("db_user_portCount")
    #db_sector_prop = db_getSectorProp("db_user_sector")
    db_pushTrade = session.prepare("INSERT INTO db_trades_stream (userId,userName,tickerName,tickerSector,tickerPrice,tradeQuantity,total_val,tradeTime,tradeType) VALUES (?,?,?,?,?,?,?,?,?) USING TTL 1036800")

    # Kafka Consumer
    KafkaStream = KafkaUtils.createDirectStream(ssc, [kafka_topic], {"bootstrap.servers":kafka_brokers})
    lines = KafkaStream.map(lambda x:x[1])
    lines.foreachRDD(self.process)
    ssc.start()
    ssc.awaitTermination()
