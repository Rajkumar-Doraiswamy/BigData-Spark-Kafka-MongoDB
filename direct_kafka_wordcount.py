"""
Last amended: 11th July, 2019
My folder: /home/kafka/Documents/kafka_spark_streaming/python_files

Ref: https://github.com/apache/spark/tree/master/examples/src/main/python/streaming

Run as:
spark-submit --jars  /opt/spark/jars/spark-streaming-kafka-0-8-assembly_2.11-2.4.3.jar  /home/kafka/direct_kafka_wordcount.py localhost:9092 test

ALSO SEE:
http://tlfvincent.github.io/2016/09/25/kafka-spark-pipeline-part-1/
CAN CONVERT TO DATAFRAME BY SPECIFYING :

here COLUMN NAMES OF COMPONENTS
rowRdd = rdd.map(lambda w: Row(word=w[0], cnt=w[1]))

"""

# Ref: Spark streaming module API
#      http://spark.apache.org/docs/2.1.0/api/python/pyspark.streaming.html

# 1.1 Import modules
import sys
# 1.2
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# 1.3 Kafka connector
from pyspark.streaming.kafka import KafkaUtils

# 1.4 Check command line arguments
if len(sys.argv) != 3:
    #print("Usage: direct_kafka_wordcount.py <broker_list> <topic>", file=sys.stderr)
    exit(-1)


########## 1. Create batches of discretised RDD objects #################

# 2. Create a sparkcontext and streaming context
#    Consider these contexts as objects that carry necessary information to set up internal services
#     and establishe connections to a Spark execution environment.
#     We create a local StreamingContext with two execution threads, and batch interval of 5 second.
sc = SparkContext("local[2]", appName="PythonStreamingDirectKafkaWordCount")
ssc = StreamingContext(sc, 5)

# 2.1 Get brokers and topic name
brokers, topic = sys.argv[1:]

# 2.2 Using this context, we can create a DStream that represents streaming data
#     from a kafka topic, given broker address:port
kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
kvs.pprint()           # Print the first num elements of each RDD generated in this DStream: pprint(num=10)

"""
(None, u'this is one line and line')
"""

########## 2. Write program to process each discrete object ##############################


# 3. Process kvs which is, in fact, RDD	using map()
#    A map is a transformation operation in Apache Spark.
#    It applies to each element of RDD and it returns the result as new RDD.
#    map transforms an RDD of length N into another RDD of length N.
lines = kvs.map(lambda x: x[1])        # Remove key from every line (key:line pair)

# 3.1
lines.pprint()                            

"""
this is one line and line
"""

# 4. flatMap (loosely speaking) transforms an RDD of length N into a collection of N collections.
lfp = lines.flatMap(lambda line: line.split(" "))
lfp.pprint()                              

"""
this
is
one
line
and
line
"""

# 5.
mapOutput = lfp.map(lambda word: (word, 1))
mapOutput.pprint()

"""
(u'this', 1)
(u'is', 1)
(u'one', 1)
(u'line', 1)
(u'and', 1)
(u'line', 1)
"""

# 6. reduceByKey For a given key, sum up with the next value of that key
#     pairs.reduceByKey((a: Int, b: Int) => a + b)
#     pairs.reduceByKey((accumulatedValue: Int, currentValue: Int) => accumulatedValue + currentValue)
#     reduceByKey takes the boilerplate of finding the key and tracking it so that you don't have to worry about managing that part.
#     https://stackoverflow.com/a/30151781

counts = mapOutput.reduceByKey(lambda a, b: a+b)
counts.pprint()

"""
(u'this', 1)
(u'and', 1)
(u'is', 1)
(u'line', 2)
(u'one', 1)
"""

########## 3. Execute process  ##############################
	
ssc.start()
ssc.awaitTermination()

#########################################
