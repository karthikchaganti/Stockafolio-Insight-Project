###############--------------------------------------------------###############
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


# Sql context
def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']

# spark main process definition
def sparkRun(rdd):

    sqlContext = getSqlContextInstance(rdd.context)

    # Read from each row into an RDD
    rowRdd = rdd.map(lambda w: Row(uuid_trade=str(json.loads(w)["uuid_trade"]),
    timestamp=json.loads(w)["timestamp"] ,traded_stock=json.loads(w)["traded_stock"],traded_stock_price=json.loads(w)["traded_stock_price"],traded_quantity=json.loads(w)["traded_quantity"],
    trade_type=json.loads(w)["trade_type"],traded_stock_sector=json.loads(w)["traded_stock_sector"]))

    # Create a dataframe out of the RDD
    df_trades = sqlContext.createDataFrame(rowRdd)

    # Read from the dataframe
    for row in df_trades.collect():

        #duplicate partition key for one of the tables in cassandra
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

        # if the stock price is invalid due to mistakes in data gen
        tickerPrice = 15 if tickerPrice == None else tickerPrice
        total_val = tickerPrice * tradeQuantity

if __name__ == "__main__":

    file=sys.argv[1]

    # Spark Context Config
    conf = SparkConf().setAppName("StockAFolio").set("spark.cores.max", "12")

    sc = SparkContext(conf=conf)

    sqlContext = SQLContext(sc)


    json_format = [StructField("userID", StringType(), True),
            StructField("tickerName", StringType(), True),
            StructField("tickerSector", StringType(), True),
            StructField("total_val", DoubleType(), True),
            StructField("tickerSector", IntegerType(), True),
            StructField("tradeQuantity", IntegerType(), True),
            StructField("tradeType", IntegerType(), True),
            StructField("tradeTime", TimestampType(), True),
            StructField("tickerPrice", FloatType(), True)]
    df = sqlContext.read.json("path to hdfs" + file, StructType(json_format))

    rowRdd = rdd.map(lambda w: Row(uuid_trade=str(json.loads(w)["uuid_trade"]),
    rdd = rdd.map(lambda w: df)
    rdd.persist(StorageLevel.MEMORY_AND_DISK_SER)
    rdd.foreachPartition(db_pushTrade)

    # Connect to Cassandra
    server_EC2 = Cluster(['Use the Cassandra Location IP'])

    session = server_EC2.connect('stockportfolio')



    # prepares the session for pushing the latest trades into the database
    db_pushTrade = session.prepare("INSERT INTO db_trades_stream (userId,tickerName,tickerSector,tickerPrice,tradeQuantity,total_val,tradeTime,tradeType) VALUES (?,?,?,?,?,?,?,?) USING TTL 1036800")


    # Run the spark code for each of the RDDs created on the individual nodes
    messages.foreachRDD(sparkRun)

    # start the spark context
    ssc.start()

    # wait for the context termination - another spark devops!
    ssc.awaitTermination()
########---*******************************EOF**************************************************---#######
