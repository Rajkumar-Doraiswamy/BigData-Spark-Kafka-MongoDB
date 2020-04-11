#  Last amended: 11th Sep, 2018
#  Myfolder: /home/ashok/Documents/spark
# Ref: https://mingchen0919.github.io/learning-apache-spark/categorical-data.html
#      https://www.analyticsvidhya.com/blog/2016/10/spark-dataframe-and-operations/


# Objectives:
#            1. Dealing with categorical columns
#            2. Using StingIndexer, OneHotEncoder, VectorAssember



## A. Create some data

# This data frame will be used to demonstrate how to use StingIndexer, OneHotEncoder, VectorAssember
# x1 and x2 are categorical columns in strings. x3 is a categorical column in integers.
# x4 is a numerical column. y1 is a categorical column in integer.
# y2 is a column in string. 

# 1.0
import pandas as pd

# 1.1
pdf = pd.DataFrame({
        'x1': ['a','a','b','b', 'b', 'c', 'd','d'],
        'x2': ['apple', 'orange', 'orange','orange', 'peach', 'peach','apple','orange'],
        'x3': [1, 1, 2, 2, 2, 4, 1, 2],
        'x4': [2.4, 2.5, 3.5, 1.4, 2.1,1.5, 3.0, 2.0],
        'y1': [1, 0, 1, 0, 0, 1, 1, 0],
        'y2': ['yes', 'no', 'no', 'yes', 'yes', 'yes', 'no', 'yes']
    })

   

# 1.2
df = spark.createDataFrame(pdf)
type(df)	# pyspark.sql.dataframe.DataFrame



# B. About DataFrame
# Ref: https://s3.amazonaws.com/assets.datacamp.com/blog_assets/PySpark_SQL_Cheat_Sheet_Python.pdf
# 2.0
df.show()	        # Show data
df.head()
df.take(2)	        # Show two rows
type(df.take(2))	# List of objects: pyspark.sql.types.Row
r = df.take(2)
r[0]			# First row
type(r[0])		# pyspark.sql.types.Row
df.describe().show()	# Summary statistics



## C. StringIndexer

# StringIndexer maps a string column to a index column that will be treated as a categorical column by spark.
#  The indices start with 0 and are ordered by label frequencies. If it is a numerical column, the column will
#   first be casted to a string column and then indexed by StringIndexer.
#    There are three steps to implement the StringIndexer
#      i.   Build the StringIndexer model: specify the input column and output column names.
#      ii)  Learn the StringIndexer model: fit the model with your data.
#      iii) Execute the indexing: call the transform function to execute the indexing process.


# 3.0
from pyspark.ml.feature import StringIndexer

# 3.1
# build indexer. No need to specify dataframe here, just column names
string_indexer = StringIndexer(inputCol='x1', outputCol='indexed_x1')  


# 3.2 Learn/fit the model on dataframe
si_model = string_indexer.fit(df)

# 3.3 Transform the data to a new DataFrame
df_si = si_model.transform(df)

# 3.4 Resulting df
#     From the result it can be seen that (a, b, c) in column x1 are converted to
#     (1.0, 0.0, 2.0). They are ordered by their frequencies in column x1.
df_si.show()


## D. OneHotEncoder
# OneHotEncoder maps a column of categorical indices to a column of of binary vectors.
#  Each index is converted to a vector. However, in spark the vector is represented by a
#  sparse vector, becase sparse vector can save a lot of memory.
#   The process of using OneHotEncoder is different to using StingIndexer. There are only two steps.
#    i) Build an indexer model
#    ii) Execute the indexing by calling transform


# 4.0
from pyspark.ml.feature import OneHotEncoder

# 4.1 Build indexer. Only specify the input/output columns.
onehotencoder = OneHotEncoder(inputCol='indexed_x1', outputCol='onehotencoded_x1')

# 4.2 Transform df_si DataFrame to df_dummy
df_dummy = onehotencoder.transform(df_si)

# 4.3 Resulting df
# (3,[2],[1.0])  => Vector length: 3, At second   position, value is 1	=   0 1 0 0
# (3,[0],[1.0]) => Vector length: 3,  At 0th      position, value is 1	=  1 0 0 0	
#  (3,[],[])	=> Vector length:3    At 3rd/last position, value is 1	=  0 0 0 1	
df_dummy.show()


## E. Process all categorical columns with Pipeline
#     A Pipeline is a sequence of stages. A stage is an instance which has the property of either fit()
#      or transform(). When fitting a Pipeline, the stages get executed in order. The example below shows
#       how to use pipeline to process all categorical columns.

# 5. List all categorical columns
categorical_columns = ['x1', 'x2', 'x3']

##=== build stages ======
# 5.1
stringindexer_stages = [StringIndexer(inputCol=c, outputCol='stringindexed_' + c) for c in categorical_columns]
# 5.2
onehotencoder_stages = [OneHotEncoder(inputCol='stringindexed_' + c, outputCol='onehotencoded_' + c) for c in categorical_columns]
# 5.3
all_stages = stringindexer_stages + onehotencoder_stages

## 5.4 Build pipeline model
# 5.4.1
from pyspark.ml import Pipeline

# 5.4.2
pipeline = Pipeline(stages=all_stages)

## 5.5 Fit pipeline model
pipeline_mode = pipeline.fit(df)

## 5.6 Transform data
df_coded = pipeline_mode.transform(df)
df_coded

## 6. Remove uncoded columns
selected_columns = ['onehotencoded_' + c for c in categorical_columns] + ['x4', 'y1', 'y2']
df_coded = df_coded.select(selected_columns)
df_coded.show()

###################################################

stages = []

for i in cat_cols:
    si = StringIndexer(inputCol=i, outputCol = i + "index")
    ohe= OneHotEncoder(inputCol=i+"index", outputCol = i+"trans")
    stages.append(si)
    stages.append(ohe)

stages
 	
