# Last amended: 25th October, 2018
# My folder: /home/ashok/Documents/spark/streaming/1.readfromnetwork
#  VM: lubuntu_spark

"""
Ref: https://github.com/apache/spark/blob/v2.3.2/examples/src/main/python/sql/streaming/structured_network_wordcount_windowed.py
Re:  https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#window-operations-on-event-time

 Counts words in UTF8 encoded, '\n' delimited text received from the network over a
 sliding window of configurable duration. Each line from the network is tagged
 with a timestamp that is used to determine the windows into which it falls.
 Usage: structured_network_wordcount_windowed.py <hostname> <port> <window duration>
   [<slide duration>]
 <hostname> and <port> describe the TCP server that Structured Streaming
 would connect to receive data.
 <window duration> gives the size of window, specified as integer number of seconds
 <slide duration> gives the amount of time successive windows are offset from one another,
 given in the same units as above. <slide duration> should be less than or equal to
 <window duration>. If the two are equal, successive windows have no overlap. If
 <slide duration> is not provided, it defaults to <window duration>.

# Usage:
#    Steps:
#		     1.  Start hadoop
#		      2. On one terminal, first run this program as:
#			      $ nc -lk 9999
#                  3. Next, on another terminal type:
#                        $ cd ~
#                        $ spark-submit spark-submit /home/ashok/Documents/spark/streaming/readfromnetwork/network_wordcount_timeWindow.py localhost 9999 10 5
##
##


"""
from __future__ import print_function

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import window

if __name__ == "__main__":
    if len(sys.argv) != 5 and len(sys.argv) != 4:
        msg = ("Usage: structured_network_wordcount_windowed.py <hostname> <port> "
               "<window duration in seconds> [<slide duration in seconds>]")
        print(msg, file=sys.stderr)
        exit(-1)

    host = sys.argv[1]
    port = int(sys.argv[2])
    windowSize = int(sys.argv[3])
    slideSize = int(sys.argv[4]) if (len(sys.argv) == 5) else windowSize
    if slideSize > windowSize:
        print("<slide duration> must be less than or equal to <window duration>", file=sys.stderr)
    windowDuration = '{} seconds'.format(windowSize)
    slideDuration = '{} seconds'.format(slideSize)

    spark = SparkSession\
        .builder\
        .appName("StructuredNetworkWordCountWindowed")\
        .getOrCreate()

    # Create DataFrame representing the stream of input lines from connection to host:port
    lines = spark\
        .readStream\
        .format('socket')\
        .option('host', host)\
        .option('port', port)\
        .option('includeTimestamp', 'true')\
        .load()

    # Split the lines into words, retaining timestamps
    # split() splits each line into an array, and explode() turns the array into multiple rows
    words = lines.select(
        explode(split(lines.value, ' ')).alias('word'),
        lines.timestamp
    )

    # Group the data by window and word and compute the count of each group
    windowedCounts = words.groupBy(
        window(words.timestamp, windowDuration, slideDuration),
        words.word
    ).count().orderBy('window')

    # Start running the query that prints the windowed word counts to the console
    query = windowedCounts\
        .writeStream\
        .outputMode('complete')\
        .format('console')\
        .option('truncate', 'false')\
        .start()

query.awaitTermination()