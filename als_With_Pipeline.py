## Last amended: 02/02/2018
## Myfolder: /home/ashok/Documents/spark/allstatesseverityclaims
##           /cdata/allstatesseverityclaims
##
## Datasource: Kaggle competitions
##             https://www.kaggle.com/c/allstate-claims-severity

## Problem: Predict extent of insurance claim
#  Ref: https://docs.databricks.com/spark/latest/mllib/binary-classification-mllib-pipelines.html
#
#
## For details and comments pl see file: /cdata/adult/adult_Without_Pipeline.py
## It is expected that data is stored in 'als' table under 'als' database
## See file: als_py_initialSteps.txt

## Start hadoop and then pyspark:
##		$ cd ~/
##		$ pyspark

########################################################

## 1.0 Call libraries
# Ref :http://spark.apache.org/docs/2.2.0/api/python/pyspark.ml.html#pyspark.ml.Pipeline
from pyspark.ml.feature import  StringIndexer, OneHotEncoder
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.regression import  RandomForestRegressor



## 2. Read data
#  2.1 Read the 'als' dataset from spark-sql store ('als' database)
#      Return the data as a spark DataFrame
df = spark.table("als.als")
x = df.dtypes
categoricalColumns =[]
for i in range(len(x)):
    if(x[i][1] == "string"):
        categoricalColumns.append(x[i][0])


# 3. Categorical Columns
categoricalColumns		# List of categorical columns
len(categoricalColumns)		# 8 + 1



# 4 An empty 'ToDo' list for feeding into pipeline
ToDo = [] 


# 5 Prepare pipeline stage, column by column ie for every column
for cc in categoricalColumns:
  stringIndexer = StringIndexer(inputCol= cc, outputCol= cc +"Index")
  encoder = OneHotEncoder(inputCol= cc + "Index", outputCol= cc + "classVec")
  ToDo = ToDo + [stringIndexer, encoder]   



# 5.1
ToDo		# twice number of column
len(ToDo)	# 234


# 6. Numerical columns
numericCols = [ i[0]   for i in df.dtypes  if i[1] != "string" ]

# 6.1
numericCols
len(numericCols)	# 15


# 7.  Generate again same names (same as finally given by OneHotEncoder) 
#       for categoricalColumns
OneHotNames = map(lambda c: c + "classVec", categoricalColumns)    
OneHotNames = list(OneHotNames)
OneHotNames         # List of names: 117


# 7.1. Compile list of all OneHot and numerical cols for assembling into 'features'
assemblerInputs = OneHotNames + numericCols[ : 14]    # Exclude 'loss'
# 7.2
assemblerInputs
len(assemblerInputs)		# 131


# 7.3 Create an object to apply to assemblerInputs, VectorAssembler
#    OutCol is 'always' has a name 'features'. 'features' contains
#     all predictors
# 7.4 Transform all features into a vector using VectorAssembler
assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")
assembler
ToDo.append(assembler)
ToDo
len(ToDo)                     # 235

# 8. Create a Pipeline.
pipeline = Pipeline(stages=ToDo)

# 8.1 Run the feature transformations.
#  - fit() computes feature statistics as needed (NOTE: 'as needed')
#  - transform() actually transforms the features.
pipelineModel = pipeline.fit(df)
df = pipelineModel.transform(df)	

df.columns
df.dtypes
len(df.columns)    


######################### CC.Modeling Data #########################################

# 9. Keep only the two relevant columns
#    Note now we need just two columns: label and features
#      You can expt below by removing other cols (cols)
selectedcols = ["loss", "features"]     # ie 15 + 2 =17 & ignore others
df = df.select(selectedcols)
df.columns


# 10. Randomly split data into training and test sets. set seed for reproducibility
(trainingData, testData) = df.randomSplit([0.5, 0.5], seed = 100)

# 10.1
trainingData.count()
testData.count()


# 11. Modeling and predictions
#     Takes very long time
rf = RandomForestRegressor(labelCol="loss",
                           featuresCol="features",
                           numTrees=4,
                           maxDepth=3,
                           seed=42)
                           
rf_model = rf.fit(trainingData)
# 11.1 Predictions
predictions = rf_model.transform(testData)
# 11.2 Actual values
testData.select("loss").show()


#######################################################################
