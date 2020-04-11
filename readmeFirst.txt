# Last amended:  29th October, 2018
# My folder: /home/ashok/Documents/spark/streaming/streamfromfiles
# VM: lubuntu_spark

=================================================
Experiment 1 Stream data into spark from network socket
=================================================

Files folder: /home/ashok/Documents/spark/streaming/readfromnetwork

# Usage:
#    Steps:
#		     1.  Start hadoop
#		      2. On one terminal, first run this program as:
#			      $ nc -lk 9999
#                  3. Next, on another terminal type:
#                        $ cd ~
#                        $ spark-submit /home/ashok/Documents/spark/streaming/readfromnetwork/network_wordcount_dataframe.py localhost 9999  >  /home/ashok/Documents/results.txt


================================================================================
Experiment 2 Source: Stream data into spark from files use dataframe operations to aggregate
================================================================================

Files folder: /home/ashok/Documents/spark/streaming/streamfromfiles

# Usage Steps:
#		     Step 1.     Start hadoop
#		      Step 2.   On one terminal, first run the file-generator program as:
#
#			                 $ cd /home/ashok/Documents/spark/streaming/streamfromfiles
#			                 $  bash file_gen.sh
#
#                  Step3.  Next, open another terminal and  type:
#                    $ cd ~
#                    $ spark-submit /home/ashok/Documents/spark/streaming/streamfromfiles/stream_data_fromfiles1.py
#                   OR
#                    $ spark-submit /home/ashok/Documents/spark/streaming/streamfromfiles/stream_data_fromfiles1.py  > /home/ashok/Documents/streamresults.txt

#
#	Then examine results.txt and you will find counts
#         /home/ashok/Documents/streamresults.txt

===================================================
Experiment 3 Use SQL (and not dataframe) for aggregations
===================================================

Files folder: /home/ashok/Documents/spark/streaming/streamfromfiles

# Usage Steps:
#		     Step 1.     Start hadoop
#		      Step 2.   On one terminal, first run the file-generator program as:
#
#			                 $ cd /home/ashok/Documents/spark/streaming/streamfromfiles
#			                 $  bash file_gen.sh
#
#                  Step3.  Next, open another terminal and  type:
#                    $ cd ~
#                    $ spark-submit /home/ashok/Documents/spark/streaming/streamfromfiles/stream_data_fromfiles_sql.py
#                   OR
#                    $ spark-submit /home/ashok/Documents/spark/streaming/streamfromfiles/stream_data_fromfiles_sql.py  > /home/ashok/Documents/streamresults.txt

#
#	Then examine results.txt and you will find counts
#         /home/ashok/Documents/streamresults.txt


============================================================
Experiment 4: Stream data into spark from kafka topic Spark 
#                             Spark aggregates data creates another topic and
#                             feeds data back into kafka
============================================================

Files folder: /home/ashok/Documents/spark/streaming/spark-kafka

# Usage : As described in file: spark_kafka_streaming.txt


============================================================
Experiment 5: 
### Objectives:
#					i)   Demonstrate Kafka-spark streaming
#					ii)  Use of batch-mode streaming for debugging
#					iii) Spark continuous streaming
#					iv) Joining stream data with offline (static) data
#					 v) Using kafkacat to directly publish data in kafka
#					vi) Manipulating invoice dataStream data into spark from kafka topic Spark 
============================================================
Files folder: /home/ashok/Documents/spark/streaming/streaminvoicedata

# Usage : As described in file: spark_kafka_invoice_stream.txt



====================================================================
Experiment 6: On line Sentiment Analysis: Sentences are streaming 
#                             into Spark  from kafka & Spark analyses & provides
#                             sentiment score
====================================================================

Files folder: /home/ashok/Documents/spark/streaming/kafka_spark_streaming

# Objective On line Sentiment Analysis
# Usage as described in :    kafka_spark_streaming/howToProceed.txt



==============
Experiment 7
==============
Files folder: /home/ashok/Documents/spark/streaming/streamfromfiles

# Usage Steps:
#		     Step 1.     Start hadoop
#		      Step 2.   On one terminal, first run the file-generator program as:
#
#			                 $ cd /home/ashok/Documents/spark/streaming/streamfromfiles
#			                 $  bash file_gen.sh
#
#                  Step3.  Next, open another terminal and  type:
#                    $ cd ~
#                    $ spark-submit /home/ashok/Documents/spark/streaming/streamfromfiles/stream_data_fromfiles2.py
#                   OR
#                    $ cd /home/ashok
#                    $ spark-submit /home/ashok/Documents/spark/streaming/streamfromfiles/stream_data_fromfiles2.py  > /home/ashok/Documents/streamresults.txt

#
#	Then examine results.txt and you will find counts
#         /home/ashok/Documents/streamresults.txt

===========================================================================================
