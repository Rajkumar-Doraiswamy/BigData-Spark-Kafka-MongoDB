# Last amended: 20th August, 2019
# Ref: https://spark.apache.org/docs/latest/streaming-programming-guide.html#a-quick-example
# My folder: /home/ashok/Documents/spark/streaming/1.readfromnetwork
# Simple Spark streaming
#VM: lubuntu_spark.vdi
#
# Objectives:
#		1. Word count: Count the number of words in a sentence
#		                                coming over the network
#             2. Run first Quick code below to understand
#
# Usage:
#		     1. Start hadoop
#		         On one terminal, run this program as:
#
#  cd ~
 #    $ spark-submit  /home/ashok/Documents/spark/streaming/1.readfromnetwork/1.network_wordcount.py localhost 9999  >  /home/ashok/Documents/results.txt
#
#                2. On another terminal type the following and then type out few sentences
#			$ nc -lk 9999
#
#	Then printout file,  results.txt, and you will find word-counts
#
#            ELSE:
#		3. You can also run complete example from:  pyspark (ipython)
#                  But, later kill the ipython (pyspark) process, as:    
# 
#                      ps aux | grep ipython
#                      kill  -9 ipython
#
"""
Broad Steps:

After a streaming context is defined, one has to do the following.

    i)   Write the input sources by creating input DStreams
    ii)  Write the streaming computations by applying transformation
          and output operations to DStreams.
    iii) Start receiving data and processing it using
          streamingContext.start().
    iv) Stop processing using streamingContext.awaitTermination().
    v)  The processing can be manually stopped using streamingContext.stop().

"""


##################

# 1.0 Call libraries
from pyspark import SparkContext     					# Not needed in pyspark

# 2.0 Create a local StreamingContext with 2 working threads and
#         batch interval of 5 seconds

sc = SparkContext("local[2]", "NetworkWordCount")    # Not needed in pyspark

# 2.1  TO RUN ON PYSPARK START HERE
from pyspark.streaming import StreamingContext
ssc = StreamingContext(sc, 5)

# 3.0 Create a DStream that will connect to TCP source specified 
#         as hostname:port, like localhost:9999. The complete input 
#         whether on one line  or two lines or multiple lines is read as
#         one string.

lines = ssc.socketTextStream("localhost", 9999)

# 4. Split each string into words. flatMap creates
#     another DStream:

words = lines.flatMap(lambda line: line.split(" "))

# 5. For every word, create a pair (word,1).

pairs = words.map(lambda word: (word, 1))

# 6. For every pair, add values:

wordCounts = pairs.reduceByKey(lambda x, y: x + y)

# 7. Print the first ten elements of each RDD generated 
#     in this DStream to the console

wordCounts.pprint()

# 8.0   Start the process:

ssc.start()             			  # Start the computation
ssc.awaitTermination()	  # Wait for the computation to terminate



##########################################

# Quick code 
# Q1.0
lines = ["Good morning. Nice day", "OK bye bye", "Good work", "Good day"]
# Q2.0
lines = sc.parallelize(lines)      # Convert lines to RDD

# Q3.0
# https://data-flair.training/blogs/apache-spark-map-vs-flatmap/

words = lines.flatMap(lambda line: line.split(" "))    # Split each line on space and flatten inner lists
words.collect()        			                                                  #      ['Good', 'morning.', 'Nice', 'day', 'OK', 'bye', 'bye']

# Q4.0
pairs = words.map(lambda word: (word, 1))  
pairs.collect()      			    # A list of tuples: [('Good', 1), ('morning.', 1),...]

# Q5.0 Sum up values (y), one by one, and accumulate in accumlator (x)
# Ref: https://stackoverflow.com/questions/30145329/reducebykey-how-does-it-work-internally
#   wordCounts pairs.reduceByKey((accumulatedValue: Int, currentValue: Int) => accumulatedValue + currentValue)

wordCounts = pairs.reduceByKey(lambda x, y: x + y)
wordCounts.collect()        # [('bye', 2), ('morning.', 1), 

#######################################################



