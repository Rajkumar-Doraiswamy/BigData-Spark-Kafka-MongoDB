# Last amended: 11th July, 2019
# Myfolder: /home/ashok/Documents/spark/streaming/1.readfromnetwork
# Ref: https://spark.apache.org/docs/latest/streaming-programming-guide.html#a-quick-example
# Ref: https://github.com/apache/spark/blob/master/docs/structured-streaming-programming-guide.md
# Simple Spark streaming. Output is a DataFrame
#
# Objective:
#		1. Word count: Count the number of words in a sentence
#		     coming over the network--Use spark structured streaming
#                   Output is in a DataFrame format

# Usage:
#    Steps:
#		     1.  Start hadoop
#		      2. On one terminal, first run this program as:
#			      $ nc -lk 9999
#
#                  3. Next, on another terminal type:
#                        $ cd ~
#                        $ spark-submit /home/ashok/Documents/spark/streaming/readfromnetwork/network_wordcount_dataframe.py localhost 9999  >  /home/ashok/Documents/results.txt
#
#		    4. Then examine file, results.txt, and you will find word-counts


# 1.0 Call libraries to create SparkContext and session
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

# 1.1 Call some functions
from pyspark.sql.functions import split, explode

# 2.0 Start spark session
#         Receive streams every 1 second
#         SEE NOTE BELOW
sc = SparkContext('local',1)    
spark = SparkSession(sc )

# 2.1 Start reading data from socket at port 9999
#        'lines' DataFrame represents an unbounded table containing the
#         streaming text data. This table contains one column of strings
#         named “value”, and each line in the streaming text data becomes 
#         a row in the table. Note, that this is not currently receiving any
#        data as we are just setting up the transformation, and have not 
#        yet started it. 

lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

# 2.2 Process read lines
#         explode:  arranges an array, one per row
#        We have used two built-in SQL functions - split and explode,
#        to split each line into multiple rows with a word each.
#        In addition, we use the function alias to name the new 
#        column as “word”.

words = lines.select( explode( split(lines.value, " ") ).alias("word") )

# 2.3  Set up the query on the streaming data. 
#         Group by 'word' and count
#         Finally, we have defined the wordCounts DataFrame by grouping
#         by the unique values in the Dataset and counting them. Note that this
#         is a streaming DataFrame which represents the running word counts 
#         of the stream ie it will emit many values.

wordCounts = words.groupBy("word").count()

# 3.0 Publish on console and start streaming
#        All that is left is to actually start receiving 
#        data and computing the counts. To do this,
#        we set it up to print the complete set of 
#        counts (specified by outputMode("complete"))
#        to the console every time they are updated. 
#        And then start the streaming computation using start().

#       After this code is executed, the streaming computation
#       will have started in the background.

pub = wordCounts.writeStream.outputMode("complete").format("console").start()

# 3.1 Kill process on termination signal
#       The 'pub' object is a handle to that active streaming query,
#       and we have decided to wait for the termination of the query
#       using awaitTermination() to prevent the process from exiting
#       while the query is active.

pub.awaitTermination()

################  FINISH ##########################
# Both sparkcontext and sparksession can be merged, as:
# Ref: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#quick-example
#spark = SparkSession \
#    .builder \
#    .appName("StructuredNetworkWordCount") \
#    .getOrCreate()
#    Try it afterwards
##################################################


