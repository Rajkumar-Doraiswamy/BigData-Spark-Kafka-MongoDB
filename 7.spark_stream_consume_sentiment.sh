#!/bin/bash


# To download necessary libraries, run:
#pyspark --packages org.apache.spark:spark-streaming-kafka-0-8-assembly_2.11:2.3.1

echo "Starting spark streaming job that consumes messages posted under topic test"
echo "Results of streaming are being redirected to ~/useless.txt"
echo "This terminal will remain engaged..."
echo "wait.... "
sleep 15

## OLD CODE
#spark-submit --jars  /opt/spark/jars/spark-streaming-kafka-0-8-assembly_2.11-2.2.1.jar  /home/ashok/Documents/spark/kafka_spark_streaming/python_files/direct_kafka_wordcount.py #localhost:9092 test    > /home/ashok/useless.txt

#spark-submit --jars  /opt/spark-2.3.1-bin-hadoop2.7/jars/spark-streaming-kafka-0-8-assembly_2.11-2.3.1.jar  /home/ashok/Documents/spark/streaming/kafka_spark_streaming/python_files/direct_kafka_wordcount_sentiment.py localhost:9092 test > /home/ashok/useless.txt



## Present code
spark-submit   --packages org.apache.spark:spark-streaming-kafka-0-8-assembly_2.11:2.4.3  /home/ashok/Documents/spark/streaming/kafka_spark_streaming/python_files/direct_kafka_wordcount_sentiment.py localhost:9092 test > /home/ashok/useless.txt
