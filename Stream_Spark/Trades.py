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

# Spark Context Config
conf = SparkConf().setAppName conf = SparkConf().setAppName("StockAFolio").set("spark.cores.max", "12")
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 1) # Window 1 seconds
zkQuorum = "localhost:2181"
kafka_topic = "StockStream"
kafka_brokers = "ec2-34-198-10-253.compute-1.amazonaws.com:9001"


# Kafka Consumer
# Map each message the consumer sends with a val:1 and map it up!
kafkaStream =  kafkaStream = KafkaUtils.createStream(ssc,zkQuorum,kafka_brokers,kafka_topic)


def process(rdd):



static createStream(ssc, zkQuorum, groupId, topics, kafkaParams=None, storageLevel=StorageLevel(True, True, False, False, 2), keyDecoder=<function utf8_decoder at 0x7f83903ff488>, valueDecoder=<function utf8_decoder at 0x7f83903ff488>)[source]
Create an input stream that pulls messages from a Kafka Broker.

Parameters:
ssc – StreamingContext object
zkQuorum – Zookeeper quorum (hostname:port,hostname:port,..).
groupId – The group id for this consumer.
topics – Dict of (topic_name -> numPartitions) to consume. Each partition is consumed in its own thread.
kafkaParams – Additional params for Kafka
storageLevel – RDD storage level.
keyDecoder – A function used to decode key (default is utf8_decoder)
valueDecoder – A function used to decode value (default is utf8_decoder)
Returns:
A DStream object
