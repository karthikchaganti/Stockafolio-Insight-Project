###############--------------------------------------------------###############
# Trades.py : Used on Spark streaming
# Author: Karthik Chaganti
# Technology : Python
###############--------------------------------------------------###############

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


def st_getcounter(table):
        value_query = "SELECT tickerQuant, tickerValue FROM " + table + " WHERE userId = ? AND tickersector = ? AND tickerName = ?"
        st_value_query = session.prepare(value_query)
        return st_value_query

def st_getcounter1(table):
        count_query = "SELECT portfolio_count, portfolio_value FROM " + table + " WHERE userId = ?"
        st_count_query = session.prepare(count_query)
        return st_count_query


def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']

def sparkRun(rdd):
    sqlContext = getSqlContextInstance(rdd.context)
    rowRdd = rdd.map(lambda w: Row(uuid_trade=str(json.loads(w)["uuid_trade"]),
    timestamp=json.loads(w)["timestamp"] ,traded_stock=json.loads(w)["traded_stock"],traded_stock_price=json.loads(w)["traded_stock_price"],traded_quantity=json.loads(w)["traded_quantity"],
    trade_type=json.loads(w)["trade_type"],traded_stock_sector=json.loads(w)["traded_stock_sector"]))
    df_trades = sqlContext.createDataFrame(rowRdd)
    print(df_trades)
    for row in df_trades.collect():
        dupkey = 1
        stsp_timestamp  = row.timestamp
        tradeTime  = datetime.strptime(stsp_timestamp, "%Y-%m-%d %H:%M:%S")
        tradeDate = tradeTime.date()
        userId = row.uuid_trade
        userId = uuid.UUID(userId)
        tradeType = row.trade_type
        tickerName = row.traded_stock
        tickerSector = row.traded_stock_sector
        tickerPrice = row.traded_stock_price
        tradeQuantity = row.traded_quantity
        tickerPrice = 15 if tickerPrice == None else tickerPrice
        total_val = tickerPrice * tradeQuantity
        # Push the trade to the trade history database
        session.execute(db_pushTrade,(userId,tickerName,tickerSector,tickerPrice,tradeQuantity,total_val,tradeTime,tradeType))

        # Get all the values and counts from the database for the uses below
        row_val = session.execute(ses_val,(userId,tickerSector,tickerName, ))
        row_cnt =  session.execute(ses_count,(userId, ))
        row_stck_quant = 0 if not row_val else row_val[0].tickerquant
        row_stck_value = 0 if not row_val else row_val[0].tickervalue
        row_portfolio_count = 0 if not row_cnt else row_cnt[0].portfolio_count
        row_portfolio_value = 0 if not row_cnt else row_cnt[0].portfolio_value

        if(tradeType == 'SOLD'):
            tradeQuantity = -(tradeQuantity)

        if row_stck_quant < 0:
            if tradeQuantity <0:
                row_portfolio_count = row_portfolio_count + abs(tradeQuantity)
            else:
                new_stock = row_stck_quant + tradeQuantity
                if new_stock <= 0:
                    row_portfolio_count = tradeQuantity - abs(tradeQuantity)
                else:
                    row_portfolio_count = tradeQuantity - abs(row_stck_quant) + (row_stck_quant + tradeQuantity)
        elif row_stck_quant>0:
            if tradeQuantity >=0:
                row_portfolio_count = row_portfolio_count+tradeQuantity
            else:
                new_stock = row_stck_quant+tradeQuantity
                if new_stock >=0:
                    row_portfolio_count = row_portfolio_count+tradeQuantity
                else:
                    subtract_existing = -tradeQuantity
                    row_portfolio_count = row_portfolio_count-row_stck_quant+((subtract_existing - row_stck_quant))
        else:
            row_portfolio_count = row_portfolio_count + abs(tradeQuantity)

        # Update the user's stock quantity
        row_stck_quant = row_stck_quant + tradeQuantity
        row_stck_value = row_stck_value + (tradeQuantity * tickerPrice)
        row_portfolio_value = row_portfolio_value + (tradeQuantity * tickerPrice)

        session.execute(db_pushTotalCount,(userId,row_portfolio_count,row_portfolio_value))
        session.execute(db_pushDummyCount,(dupkey,userId,row_portfolio_count,row_portfolio_value))
        session.execute(db_pushStockCount,(userId,tickerSector,tickerName,row_stck_quant,row_stck_value))

    ########---*********************************************************************************---#######
if __name__ == "__main__":
    # Spark Context Config
    conf = SparkConf().setAppName("StockAFolio").set("spark.cores.max", "12")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 1) # Window 1 seconds

    zkQuorum = "localhost:2181"
    kafka_topic = "StreamingTrades"
    kafka_brokers = "Use the Kafka Location IP"
    ########---*********************************************************************************---#######
    # Connect to Cassandra
    server_EC2 = Cluster(['Use the Cassandra Location IP'])
    session = server_EC2.connect('stockportfolio')
    ########---*********************************************************************************---#######
    ses_val = st_getcounter("db_user_portfolio")
    ses_count = st_getcounter1("db_user_portCount")
    # prepares the session for pushing the latest trades into the database
    db_pushTrade = session.prepare("INSERT INTO db_trades_stream (userId,tickerName,tickerSector,tickerPrice,tradeQuantity,total_val,tradeTime,tradeType) VALUES (?,?,?,?,?,?,?,?) USING TTL 1036800")
    db_pushTotalCount = session.prepare("INSERT INTO db_user_portCount(userId,portfolio_count,portfolio_value) VALUES (?,?,?)")
    db_pushStockCount = session.prepare("INSERT INTO db_user_portfolio(userId,tickerSector,tickerName,tickerQuant,tickerValue) VALUES (?,?,?,?,?)")
    db_pushDummyCount = session.prepare("INSERT INTO db_user_DummyCount(dupkey,userId,portfolio_count,portfolio_value) VALUES (?,?,?,?)")
    # Kafka Consumer
    KafkaStream = KafkaUtils.createDirectStream(ssc, [kafka_topic], {"bootstrap.servers":kafka_brokers})
    messages = KafkaStream.map(lambda x:x[1])
    messages.foreachRDD(sparkRun)
    ssc.start()
    ssc.awaitTermination()
########---*******************************EOF**************************************************---#######
