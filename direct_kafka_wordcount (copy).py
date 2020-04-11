"""
Last amended: 6th April, 2018
My folder: /home/kafka/Documents/kafka_spark_streaming/python_files

Ref: https://github.com/apache/spark/tree/master/examples/src/main/python/streaming

Run as:
spark-submit --jars  /opt/spark/jars/spark-streaming-kafka-0-8-assembly_2.11-2.2.1.jar  /home/kafka/direct_kafka_wordcount.py localhost:9092 test



"""

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

if len(sys.argv) != 3:
    #print("Usage: direct_kafka_wordcount.py <broker_list> <topic>", file=sys.stderr)
    exit(-1)

########## 1. Create batches of discretised RDD objects #################
# Create a StreamingContext (ssc) with batch interval of 5 seconds
sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
ssc = StreamingContext(sc, 5)

brokers, topic = sys.argv[1:]

# Using this context, we can create a DStream that represents streaming data
# from a kafka topic, given broker address:port
kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})

########## 2. Write program to process each discrete object ##############################

# Process kvs which is, in fact, RDD	
lines = kvs.map(lambda x: x[1])        # Remove key from every line (key:line pair)

counts = lines.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a+b)

counts.pprint()

########## 3. Execute process  ##############################
	
ssc.start()
ssc.awaitTermination()

#########################################
