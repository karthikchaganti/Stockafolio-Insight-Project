# Spark Streaming Code
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


def st_getcounter(table):
        value_query = "SELECT tickerQuant, tickerValue FROM " + table + " WHERE userId = ? AND tickerName = ?"
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
        stsp_timestamp  = row.timestamp
        tradeTime  = datetime.strptime(stsp_timestamp, "%Y-%m-%d %H:%M:%S")
        #userName = row.userName_trade
        userId = row.uuid_trade
        userId = uuid.UUID(userId)
        tradeType = row.trade_type
        tickerName = row.traded_stock
        tickerSector = row.traded_stock_sector
        tickerPrice = row.traded_stock_price
        tradeQuantity = row.traded_quantity
        tickerPrice = 0 if tickerPrice == None else tickerPrice
        total_val = tickerPrice * tradeQuantity
        # Push the trade to the trade history database
        session.execute(db_pushTrade,(userId,tickerName,tickerSector,tickerPrice,tradeQuantity,total_val,tradeTime,tradeType))

        # Get all the values and counts from the database for the uses below
        row_val = session.execute(ses_val,(userId,tickerName, ))
        row_cnt =  session.execute(ses_count,(userId, ))
        #with open('log.txt', 'a') as f:
            #f.write(row_val)
        #row_prop = session.execute(ses_prop,(userId,tickerSector))

        row_stck_quant = 0 if not row_val else row_val[0].tickerquant
        row_stck_value = 0 if not row_val else row_val[0].tickervalue
        row_portfolio_count = 0 if not row_cnt else row_cnt[0].portfolio_count
        row_portfolio_value = 0 if not row_cnt else row_cnt[0].portfolio_value
        #row_sec_prop = 0 if len(row_prop) == 0 else row_portfolio_vals[0].sec_prop

        if(tradeType == 'SOLD'):
            tradeQuantity = -(tradeQuantity) # if the trade is sell, then negate the volume as it needs to be subtracted

        if row_stck_quant < 0:                                  # user has taken shorts on this stock.
            if tradeQuantity <0:
                row_portfolio_count = row_portfolio_count + abs(tradeQuantity)                # since the user sold more shorts, it increases their portfolio
            else:
                # since he purchased new shares on this stock, see if these purchased stocks cover the shorts                                           # if he rather purchased
                new_stock = row_stck_quant + tradeQuantity
                if new_stock <= 0: # if not, less shares are compensated for the shorts and hence reduce the same no. from the portfolio
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
        #if row_stck_value !=0 and row_portfolio_value!=0:
            #row_sec_prop = abs(row_stck_value) / float(row_portfolio_value)
        #else:
            #row_sec_prop = 0 + row_sec_prop

        session.execute(db_pushTotalCount,(userId,row_portfolio_count,row_portfolio_value))
        session.execute(db_pushStockCount,(userId,tickerName,row_stck_quant,row_stck_value))
        #session.execute(db_user_sector,(userId,row_sec_prop,tickerSector))

    ########---*********************************************************************************---#######
if __name__ == "__main__":
    # Spark Context Config
    conf = SparkConf().setAppName("StockAFolio").set("spark.cores.max", "12")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 1) # Window 1 seconds

    zkQuorum = "localhost:2181"
    kafka_topic = "StreamTrades"
    kafka_brokers = "ec2-34-197-245-192.compute-1.amazonaws.com:9092"
    ########---*********************************************************************************---#######
    # Connect to Cassandra
    server_EC2 = Cluster(['ec2-34-198-185-77.compute-1.amazonaws.com'])
    session = server_EC2.connect('stockportfolio')
    ########---*********************************************************************************---#######



    ses_val = st_getcounter("db_user_portfolio")
    ses_count = st_getcounter1("db_user_portCount")
    #proportion_query = "SELECT sec_prop FROM db_user_sector WHERE userId = ? AND tickerSector = ?"
    #ses_prop = session.prepare(proportion_query)

    # prepares the session for pushing the latest trades into the database
    db_pushTrade = session.prepare("INSERT INTO db_trades_stream (userId,tickerName,tickerSector,tickerPrice,tradeQuantity,total_val,tradeTime,tradeType) VALUES (?,?,?,?,?,?,?,?) USING TTL 1036800")
    db_pushTotalCount = session.prepare("INSERT INTO db_user_portCount(userId,portfolio_count,portfolio_value) VALUES (?,?,?)")
    db_pushStockCount = session.prepare("INSERT INTO db_user_portfolio(userId,tickerName,tickerQuant,tickerValue) VALUES (?,?,?,?)")
    #db_pushStockCount = session.prepare("INSERT INTO db_user_sector(userId,sec_prop,tickerSector) VALUES (?,?,?)")
    # Kafka Consumer
    KafkaStream = KafkaUtils.createDirectStream(ssc, [kafka_topic], {"bootstrap.servers":kafka_brokers})
    messages = KafkaStream.map(lambda x:x[1])
    messages.foreachRDD(sparkRun)
    ssc.start()
    ssc.awaitTermination()
