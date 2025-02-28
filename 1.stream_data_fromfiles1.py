# Last amended: 21st August,2019
# Ref: https://spark.apache.org/docs/latest/streaming-programming-guide.html#a-quick-example
# Ref: https://github.com/apache/spark/blob/master/docs/structured-streaming-programming-guide.md
# API ref: http://spark.apache.org/docs/2.1.0/api/python/pyspark.sql.html

#  Structured Spark streaming and aggregation
#
# Objective:
#		1. Analyse data streaming into spark from files
#		    Use Structured spark streaming

# Usage Steps:
#		     Step 1.     Start hadoop
#		      Step 2.   On one terminal, first run the filegenerator program as:
#
#			                 cd /home/ashok/Documents/spark/streaming/2.streamfromfiles
#                                      chmod +x file_gen.sh
#			                  ./file_gen.sh
#
#                  Step3.  Next, open another terminal and  type:
#                    $ cd ~
#                    $ spark-submit /home/ashok/Documents/spark/streaming/2.streamfromfiles/1.stream_data_fromfiles1.py
#                   OR
#                    $ spark-submit /home/ashok/Documents/spark/streaming/2.streamfromfiles/1.stream_data_fromfiles1.py  > /home/ashok/Documents/streamresults.txt

#                   ELSE
#                   run on pyspark (ipython)
#                   from para #2.1 onwards

#
#	Then examine streamresults.txt and you will find counts
#        cat  /home/ashok/Documents/streamresults.txt
#        hdfs dfs -ls  /user/ashok/data_files/fake


# Call libraries
#  1.0 Library to generate Spark context
#          whether local  or on hadoop

from pyspark.context import SparkContext

# 1.1  Library to generate SparkSession

from pyspark.sql.session import SparkSession

# 1.2 Some type to define data schema

from pyspark.sql.types import StructType

# 2.0 Create spark context and session:

sc = SparkContext('local')
spark = SparkSession(sc)

# 2.1 Where will be my csv files which spark will analyse
#       Path should be on hadoop

datafiles = 'hdfs://localhost:9000/user/ashok/data_files/fake'

# 3.0 CSV file structure:

from pyspark.sql.types import StructType
userSchema = StructType()                                                            \
                                                        .add("name", "string")             \
                                                        .add("age", "integer")               

# 4.0
# Stream files from folder /home/ashok/Documents/spark/data. 
#    New content must be added to new files.
# Ref:  https://stackoverflow.com/questions/45086501/how-to-process-new-records-only-from-file

csvDF = spark                                               \
                            .readStream                       \
                            .option("sep", ";")            \
                            .schema(userSchema)   \
                            .csv(datafiles)                  


# 4.1 Print data schema
csvDF.printSchema();


# 4.1 Perform some selection and aggregation using dataframe csvDF
#         Multiple streaming aggregations are not supported

abc = csvDF                                                          \
                      .select("name", "age")             \
                      .where("age > 40")                   

abc = abc                                                                              \
                   .groupby("name")                                          \
                   .agg({ 'age' :   'avg' ,  '*' :  'count'  })

# 4.2 Print result to console.  'append' mode also exists but is not supported when
#         there are aggregations

result = abc.writeStream                                                               \
                                                    .format("memory")                       \
                                                    .outputMode("complete")        \
                                                   .format("console")                        \
                                                   .start()


# 4.3
result.awaitTermination()

##################################################################
