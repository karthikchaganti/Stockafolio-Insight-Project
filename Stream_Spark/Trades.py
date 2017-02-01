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
    for row in df_trades.collect():
        stsp_timestamp  = row.timestamp.encode('utf-8')
        tradeTime  = datetime.strptime(stsp_timestamp, "%Y-%m-%d %H:%M:%S")
        userName = row.userName_trade
        userId = row.uuid_trade
        tradeType = row.trade_type
        tickerName = row.traded_stock
        tickerSector = row.traded_stock_sector
        tickerPrice = row.traded_stock_price
        tradeQuantity = row.traded_quantity
        total_val = tickerPrice * tradeQuantity

        # Push the trade to the trade history database
        session.execute(db_pushTrade,(userId,userName,tickerName,tickerSector,tickerPrice,tradeQuantity,total_val,tradeTime,tradeType))

if __name__ == "__main__":
    # Spark Context Config
    conf = SparkConf().setAppName("StockAFolio").set("spark.cores.max", "12")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 1) # Window 1 seconds

    zkQuorum = "localhost:2181"
    kafka_topic = "TradeStream"
    kafka_brokers = "ec2-34-198-10-253.compute-1.amazonaws.com:9001"
    # Connect to Cassandra
    server_EC2 = Cluster(['ec2-34-198-236-106.compute-1.amazonaws.com'])
    session = server_EC2.connect('stockportfolio')
    # prepares the session for pushing the latest trades into the database
    db_pushTrade = session.prepare("INSERT INTO db_trades_stream (userId,userName,tickerName,tickerSector,tickerPrice,tradeQuantity,total_val,tradeTime,tradeType) VALUES (?,?,?,?,?,?,?,?,?) USING TTL 1036800")

    # Kafka Consumer
    KafkaStream = KafkaUtils.createDirectStream(ssc, [kafka_topic], {"bootstrap_servers":kafka_brokers})
    lines = KafkaStream.map(lambda x:x[1])
    lines.foreachRDD(process)
    ssc.start()
    ssc.awaitTermination()
