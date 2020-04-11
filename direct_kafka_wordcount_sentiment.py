"""
Last amended: 11/07/2019
My folder: /home/kafka/Documents/kafka_spark_streaming/python_files

Ref: https://github.com/apache/spark/tree/master/examples/src/main/python/streaming

 Counts words in UTF8 encoded, '\n' delimited text directly received from Kafka in every 2 seconds.
 Usage: direct_kafka_wordcount.py <broker_list> <topic>

Run this file as:
spark-submit --jars  /opt/spark/jars/spark-streaming-kafka-0-8-assembly_2.11-2.2.1.jar  /home/kafka/direct_kafka_wordcount_sentiment.py localhost:9092 test > useless.txt



"""

# Import modules
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
# Import kafka connector
from pyspark.streaming.kafka import KafkaUtils


# Define a function to print results
def print_happiest_words(rdd):
    top_list = rdd.take(5)
    print("Happiest topics in the last 5 seconds (%d total):" % rdd.count())
    for tuple in top_list:
        print("%s (%d happiness)" % (tuple[1], tuple[0]))

# Main code
# Check arguments
if len(sys.argv) != 3:
    #print("Usage: network_wordjoinsentiments.py <hostname> <port>", file=sys.stderr)
    sys.exit(-1)

# Create Spark streaming context
sc = SparkContext(appName="MyPythonStreamingNetworkWordJoinSentiments")
ssc = StreamingContext(sc, 5)   # No of 

# Read in the word-sentiment dictionary and create a static RDD from it
word_sentiments_file_path = "/user/ashok/data_files/AFINN-111.txt"
#word_sentiments_file_path = "/home/ashok/Documents/spark/streaming/kafka_spark_streaming/data/AFINN-111.txt"
word_sentiments = ssc.sparkContext.textFile(word_sentiments_file_path).map(lambda line: tuple(line.split("\t")))


# Who are kafka brokers/port
brokers, topic = sys.argv[1:]
kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
lines = kvs.map(lambda x: x[1])
lines.pprint()

word_counts = lines.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
word_counts.pprint()


# Determine the words with the highest sentiment values by joining the streaming RDD
# with the static RDD inside the transform() method and then multiplying
# the frequency of the words by its sentiment value

#happiest_words = word_counts.transform(lambda rdd: word_sentiments.join(rdd)) \
#.map(lambda word_tuples: (word_tuples[0], float(word_tuples[1][0]) * word_tuples[1][1])) \
#.map(lambda word_happiness: (word_happiness[1], word_happiness[0])) \
#.transform(lambda rdd: rdd.sortByKey(False))

happiest_words = word_counts.transform(lambda rdd: word_sentiments.join(rdd)).map(lambda word_tuples: (word_tuples[0], float(word_tuples[1][0]) * word_tuples[1][1])).map(lambda word_happiness: (word_happiness[1], word_happiness[0])).transform(lambda rdd: rdd.sortByKey(False))



happiest_words.pprint()

happiest_words.foreachRDD(print_happiest_words)

ssc.start()
ssc.awaitTermination()

#########################################
