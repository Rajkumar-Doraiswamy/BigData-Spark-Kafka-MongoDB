#  Last amended: 08th Oct, 2018
#  Myfolder: /home/ashok/Documents/spark
# Ref:
# Tutorials:
#      https://changhsinlee.com/pyspark-dataframe-basics/
#      https://www.analyticsvidhya.com/blog/2016/10/spark-dataframe-and-operations/
#      https://dzone.com/articles/pyspark-dataframe-tutorial-introduction-to-datafra
# Documentation:
#      http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark-sql-module
#      http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.functions
# Cheat Sheet
#      https://s3.amazonaws.com/assets.datacamp.com/blog_assets/PySpark_SQL_Cheat_Sheet_Python.pdf


#  Objective:
#           Dataframe operations in spark cluster
#           Dealing with categorical columns
#               i) StrinIndexing them
#              ii) OneHotEncoding
#             iii) PipeLining for multiple columns


###### A. Initial operations:
# 1.0 Start hadoop
	$ ./allstart.sh
# 1.1 Transfer data files from local file system to hadoop:

	$ hdfs dfs -put /cdata/nycflights13/airports.csv hdfs://localhost:9000/user/ashok/data_files/nycflights
	$ hdfs dfs -put /cdata/nycflights13/weather.csv hdfs://localhost:9000/user/ashok/data_files/nycflights
	$ hdfs dfs -ls -h hdfs://localhost:9000/user/ashok/data_files/nycflights

# 1.2 Start pyspark. Its starting creates a context 'sc' and session 'spark'

	$ pyspark

# 1.3 Get help in pyspark:
#     Try the following two commands:

?sc
?spark


###### B. Read Dataset from hadoop
# 2.0 Read 'airports.csv file into spark from hadoop

# Where is my file? Path:
URL_of_file= "hdfs://localhost:9000/user/ashok/data_files/nycflights/"

# 2.0.1 Takes time
airports_df = spark.read.csv(URL_of_file + "airports.csv", inferSchema = True, header = True)


# 2.1 Show top-5 rows

# 2.1.1
airports_df.head(4)        	# Ouput is spark dataset (Row instance)
# 2.1.2
type(airports_df.head(4))
# 2.1.3
type(airports_df.head(4)[0])


# 2.2 Better display in columnar format

airports_df.show(5)

# 3.0 Have a look at the dataframe schema,
#     i.e. the structure of the DataFrame

airports_df.printSchema()

# 3.1 Column names

airports_df.columns

# 3.2 How many rows:

airports_df.count()

# 3.3 How many columns?

len(airports_df.columns)

# 3.4 Describe a particular column

airports_df.describe('dst').show(5)


###### C. Using Verbs


# 4 Selecting Single/Multiple Columns
#   This does now work:   airports_df['faa'].show(5)
#   You have to use 'select'
#  It is more like 'dplyr' syntax, only that instead
# of %>%, we have dot (.) .
# dplyr verbs are: 
#  select(), distinct(), count(), filter(),groupby()


# 4.1
airports_df.select('faa').show(5)

#4.2
airports_df.select('faa','dst').show(5)

# 4.3 Selecting Distinct Multiple Columns

airports_df.select('dst','tz').distinct().show()

airports_df.select('dst','tz').distinct().count()

# 4.2 Like operator:

airports_df.select('name').where("name like '%La%'").show()


# 5. Filtering data
# We use the filter command to filter our DataFrame based
#  on the condition that tz must be equal to -5 and then
#  we are calculating how many records/rows are there in
#  the filtered output.

airports_df.filter(airports_df.tz == -5) .show()
airports_df.filter(airports_df.tz == -5) .count()

# 6. Combining verbs: select, filter and distinct

airports_df.select('dst', 'tz').filter(airports_df.tz == -5).show()
airports_df.select('dst', 'tz').filter(airports_df.tz == -5).distinct().show()

# 6.1 We can filter our data based on multiple conditions (AND or OR)

airports_df.filter((airports_df.tz == -5) & (airports_df.dst=="A")).show()

# 7. groupby. Can apply sum, min, max, count

airports_df.groupby('tz').count().show()
airports_df.groupby('tz').agg({'lat' : 'mean'}).show()


# 7.1 One can take the average of columns by passing
#       an unpacked list of column names.

grObject = airports_df.groupby('tz')

avg_cols = ['lat', 'lon']
grObject.avg(*avg_cols).show()


# 7.2 To call multiple aggregation functions at once, pass a dictionary.
#         The 'key' of dictionary becomes argument to 'value'.
#                             count(*)        avg(lat)      sum(lon)

grObject.agg({'*': 'count', 'lat': 'avg', 'lon':'sum'}).show()

# 8. Create new columns in Spark using .withColumn() --mutate
#      New column: altInThousands . 
#      Product of two columns:  'alt' and  'lon' 

airports_df.withColumn('altInThousands', 
                                                      airports_df.alt*airports_df.lon  ).show()

# 9. Save the new file with additional column in parquet form

xyz = airports_df.withColumn('altInThousands', airports_df.alt*airports_df.lon)

xyz.write.parquet("hdfs://localhost:9000/user/ashok/data_files/airports_extra.parquet")

# 9.1 Delete xyz from spark

import gc
del xyz
gc.collect()    # Delete all cache also


# 9.2 Read the stored parquet file

df = spark.read.parquet("hdfs://localhost:9000/user/ashok/data_files/airports_extra.parquet")
df.show()

# 9.3 Read 'weather.csv file into spark from hadoop

URL_of_file= "hdfs://localhost:9000/user/ashok/data_files/nycflights/"

weather_df = spark.read.csv(URL_of_file + "weather.csv", inferSchema = True, header = True)

weather_df.show(3)


# 10. Joins
# Refer: http://www.learnbymarketing.com/1100/pyspark-joins-by-example/
# For example, I can join the two titanic dataframes by the column PassengerId

# 10.1
airports_df.join(weather_df, airports_df.faa==weather_df.origin).show()
# 10.2
airports_df.join(weather_df, airports_df.faa==weather_df.origin, how = 'inner').show()
# 10.3
airports_df.join(weather_df, airports_df.faa==weather_df.origin, how = 'left').show()   # Could also use 'left_outer', 'right', 'full'

# 11. Many of the operations can be accessed by writing SQL queries in spark.sql().
# To make an existing Spark dataframe usable for spark.sql(), one needs to
#   register said dataframe as a temporary table.

# 11.1 As an example, we can register the two dataframes as temp tables then
#      join them through spark.sql().

airports_df.createOrReplaceTempView('dfa_temp')
weather_df.createOrReplaceTempView('dfw_temp')


# 11.2 Simple SQL query

dfj = spark.sql('select * from dfa_temp' )
dfj.show()

# 11.3 Now the SQL join

dfj = spark.sql('select * from dfa_temp a, dfw_temp b where a.faa = b.origin' )
dfj.show()

# 12. Drop a columns

airports_df.drop('name').show()

# 12.1  Or drop multiple columns

columns_to_drop = ['name', 'lat']
airports_df.drop(*columns_to_drop).show()


###### D. Pandas dataframe

# This data frame will be used to demonstrate how to use StingIndexer, OneHotEncoder, VectorAssember
# x1 and x2 are categorical columns in strings. x3 is a categorical column in integers.
# x4 is a numerical column. y1 is a categorical column in integer.
# y2 is a column in string.

# 13
import pandas as pd
# 13.1
pdf = pd.DataFrame({
        'x1': ['a','a','b','b', 'b', 'c', 'd','d'],
        'x2': ['apple', 'orange', 'orange','orange', 'peach', 'peach','apple','orange'],
        'x3': [1, 1, 2, 2, 2, 4, 1, 2],
        'x4': [2.4, 2.5, 3.5, 1.4, 2.1,1.5, 3.0, 2.0],
        'y1': [1, 0, 1, 0, 0, 1, 1, 0],
        'y2': ['yes', 'no', 'no', 'yes', 'yes', 'yes', 'no', 'yes']
    })

# 13.2

df = spark.createDataFrame(pdf)
type(df)	# pyspark.sql.dataframe.DataFrame

# 13.3 About DataFrame
df.show()	# Show data

# 13.4 Other usual commands:

df.head()
df.take(2)	# Show two rows
type(df.take(2))	# List of objects: pyspark.sql.types.Row
r = df.take(2)
r[0]			# First row
type(r[0])		# pyspark.sql.types.Row
df.describe().show()	# Summary statistics


# 14. Dealing with categorical columns
#      a. Integer encoding them---->           StringIndexer
#      b. One hot encoding integer columns --> OneHotEncoder
#      c. Multiple categorical cols --->       PipeLine


## C. StringIndexer   ( ~ LabelEncoder in sklearn)

# 14.1 StringIndexer maps a string column to a index column that
#      will be treated as a categorical column by spark.
#      The indices start with 0 and are ordered by label frequencies.
#      If it is a numerical column, the column will first be casted
#      to a string column and then indexed by StringIndexer.
#      There are three steps to implement the StringIndexer
#          i.  Instantiate StringIndexer object: specify input col & output col names
#         ii.  Learn StringIndexer model: Fit the model with your data.
#        iii.  Execute indexing: Call transform function to execute indexing process.

# 14.2

from pyspark.ml.feature import StringIndexer

# 14.3
# Instantiate StringIndexer object.
#  Specify the input categorical column name it will encode and
#    the output  column name. Specify just column names.
#     No need to specify dataframe while instantiating

string_indexer = StringIndexer(inputCol='x1', outputCol='indexed_x1')

# 14.4
# Learn the dataframe and create model/statistical object

si_model = string_indexer.fit(df)

# 14.5
# Use the model/statistical object to transform the data
#  to a new DataFrame

df_si = si_model.transform(df)

# 14.6
# resulting df
# From the result it can be seen that (a, b, c, d) in column x1 are converted to
#   (2.0, 0.0, 3.0, 1.0). They are ordered by their frequencies in column x1.
#  Max freq coded as 0.0 and min freq coded as: 3.0

df_si.show()

# 15
## D. OneHotEncoder
# OneHotEncoder maps a column of categorical indices to a column of of binary vectors.
#  Each index is converted to a vector. However, in spark the vector is represented by a
#  sparse vector, becase sparse vector can save a lot of memory.
#   The process of using OneHotEncoder is different to using StingIndexer. There are only two steps.
#    i) Build an indexer model
#    ii) Execute the indexing by calling transform

# 15.1

from pyspark.ml.feature import OneHotEncoder

# 15.2 Instantiate OHE object. Only specify the input/output column names:

onehotencoder = OneHotEncoder(
                             inputCol='indexed_x1',
                             outputCol='onehotencoded_x1'
                             )

# 15.3 Learn and transform df_si DataFrame to df_dummy
#          One step process

df_dummy = onehotencoder.transform(df_si)
df_dummy.show()

# 15.4 Resulting df_dummy
#      Interpretation: index_x1 has four values: 0,1,2,3. To encode them in a dummy manner
#                       we build four columns-- one column for each value.

# (3,[0],[1.0])  =>  Vector index 0 to 3, At 0th  index, value is 1	=   1 0 0 0
# (3,[1],[1.0])  =>  Vector index 0 to 3, At 1st  index, value is 1	=   1 0 0 0
# (3,[2],[1.0])  => Vector index 0 to 3, At  IInd index, value is 1	=   0 0 1 0
# (3,[],[])	      =>  Vector index 0 to 3,  At IIIrd index, value is 1	=   0 0 0 1

# index_x1
#        0    First col  is 1 all other cols have values 0 =>     1 0 0 0
#             Vector length 3+1;and at [0]th position value [1.0]        (3,[0],[1.0])

#        1    IInd  col  is 1 all other cols have values 0 =>     0 1 0 0
#             Vector length 3+1;and at [1]st position value [1.0]        (3,[1],[1.0])

#        2    IIIrd col  is 1 all other cols have values 0 =>     0 0 1 0
#             Vector length 3+1;and at [2]nd position value [1.0]        (3,[2],[1.0])

#        3    IVth  col  is 1 all other cols have values 0 =>     0 0 0 1
#             Vector length 3+1;and at [3]rd position value [1.0]        (3,[],[])


# 16. Transform all categorical columns at one go:
#     Invoke for loop. For each categorical col, create a
#     StringIndexer Object and OHE object after specifying
#     necessary column names. Append results to two lists:

## 16.1  Process all categorical columns with Pipeline
#             A Pipeline is a sequence of stages. A stage is an
#               instance which has the property of either fit()
#                or transform(). When fitting a Pipeline, the stages
#                get executed in order. The example below shows
#                 how to use pipeline to process all categorical columns.

# Just see data again:
#   pdf = pd.DataFrame({
#        	'x1': ['a','a','b','b', 'b', 'c', 'd','d'],
#        	'x2': ['apple', 'orange', 'orange','orange', 'peach', 'peach','apple','orange'],
#        	'x3': [1, 1, 2, 2, 2, 4, 1, 2],
#        	'x4': [2.4, 2.5, 3.5, 1.4, 2.1,1.5, 3.0, 2.0],
#        	'y1': [1, 0, 1, 0, 0, 1, 1, 0],
#        	'y2': ['yes', 'no', 'no', 'yes', 'yes', 'yes', 'no', 'yes']
 #   })


# 16.2 List all categorical columns

categorical_columns = ['x1', 'x2', 'x3']

##=== Build stages ======
# 17. StringIndexer: Instantiate as many objects as there are categorical columns

# 17.1 Note first that instantiation of StringIndexer object
#            returns an arbitray object name.
#           Create an aribitrary object

StringIndexer(inputCol = "Narender" , outputCol = 'pmModi')

# 17.2 We make use of this automatic name generation below:

stringindexer_stages = []
for c in categorical_columns:
    x = StringIndexer(inputCol=c, outputCol='stringindexed_' + c)    
    stringindexer_stages.append(x)

stringindexer_stages 

# 17.3 Or, all in one go  using list comprehension:

stringindexer_stages = [StringIndexer(inputCol=c, outputCol='stringindexed_' + c) for c in categorical_columns]

stringindexer_stages

# 17.4
type(stringindexer_stages)          # List
type(stringindexer_stages[0])       # StringIndexer object

# 18.  Similarly for OneHotEncoding.
#         An OHE object upon instantiation creates a name: 

# 18.1  Create an aribitrary object and see its name:

OneHotEncoder(inputCol='narender', outputCol='modi' )

# 18.2 We now loop over for each categorical column to create OHE:

onehotencoder_stages = []

for c in categorical_columns:
    x = OneHotEncoder(inputCol='stringindexed_' + c, outputCol='onehotencoded_' + c)
    onehotencoder_stages.append(x)

onehotencoder_stages

# 18.3 Or, all in one go:

onehotencoder_stages = [OneHotEncoder(inputCol='stringindexed_' + c,  outputCol='onehotencoded_' + c) for c in categorical_columns]

onehotencoder_stages

# 19. Create a combined list of all actions:

all_stages = stringindexer_stages + onehotencoder_stages

# 19.1
all_stages

"""
# [StringIndexer_424cbc41dac4bc5c77d5,
#  StringIndexer_49e6af465f32a47591f5,
#  StringIndexer_4eda950f6f4e1a0106b8,
#  OneHotEncoder_4c25b19fe07b0f262fd9,
#  OneHotEncoder_4dfc943d4b655c54d66f,
#  OneHotEncoder_4a1788b98db3e48acb1f]

"""

# 20 Build pipeline object to fit and transform  all stages:

# 20.1
from pyspark.ml import Pipeline

# 20.2 Instantiate Pipeline class:
pipeline = Pipeline(stages=all_stages)

## 20.3 Fit/learn  pipeline model

pipeline_mode = pipeline.fit(df)

## 20.2 Transform data

df_coded = pipeline_mode.transform(df)

df_coded.show()

## 20.3 Select only needed  ie OHE columns and other untransformed columns:

otherColumnNames =  ['x4', 'y1', 'y2']
ohe_column_names =  ['onehotencoded_' + c  for c in categorical_columns]

# 20.4
selected_columns = otherColumnNames + ohe_column_names
df_coded = df_coded.select(selected_columns)
df_coded.show()

############# Alternatively #################################

# 21 Why have two for-loops (#17.2 and #18.2) over same cols?
#       Use one for loop (from #17 to #19 )

stages = []
cat_cols = ['x1', 'x2', 'x3']
for i in cat_cols:
    si =     StringIndexer(  inputCol= i ,  outputCol =  i + "index")
    stages.append(si)

    ohe= OneHotEncoder(inputCol= i+ "index", outputCol =  i + "trans")
    stages.append(ohe)

# 21.1
stages

#############################
