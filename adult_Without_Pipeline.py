## Last amended: 24th Jan,2018
## Myfolder: /home/ashok/Documents/spark/adult
## Datasource: Adult dataset from UCI
##             https://archive.ics.uci.edu/ml/machine-learning-databases/adult/       => Data source

## Problem: Predict income of a person from his census data
#  Ref: https://docs.databricks.com/spark/latest/mllib/binary-classification-mllib-pipelines.html
## Objectives:
##		1. Learn ML on Spark
##		2. Learn importing data files in spark
##		3. Learn data preprocessing on spark
##		4. Build ML models

## Start hadoop and then pyspark:
##		$ cd ~/
##		$ pyspark

#Steps:	
#	AA.
#	(Note: While transforming data there is NO IN-PLACE transformation by nature of hadoop)
#	1. Read data from database into DataFrame
#	2. Explore DataFrame
#	3. Transform Categorical columns to OneHotEncoded form: Use StringIndexer + OneHotEncoder
#	4. Transform Target (categorical) column to integer. Call it label: Use StringIndexer
#	5. Assemble OneHot+numerical columns into one column-vector 'features': Use VectorAssembler
#	6. Use 'label' and 'features' to build models
#
#	BB. Transformational-Pipeline 
#	1. Begin by extracting categorical columns
#	2. Loop through categorical columns
#		for each column
#			Instantiate a StringIndexer (converts to integers) & transform
#				Sample: StringIndexer(inputCol= cc, outputCol= cc +"Index")		=> One column created
#			Instantiate a OneHotEncoder object (converts to sparse vector) & transform
#				Sample: OneHotEncoder(inputCol= cc+"Index", outputCol=cc+"classVec")	=> One more column created 
#		next column
#	3. Target column is also categorical. But only StringIndex it
#	   and do not convert to OneHotEncoder format
#	4. Extract a list of numeric columns
#	5. Assemble a VectorAssembler object, using:
#		a. List of numericcols
#		b. List of OneHotEncoded columns
#		c. a+b, ie predictor columns => features
#			Sample: VectorAssembler(inputCols=assemblerInputs, outputCol="features")
#	6. Transform df also using VectorAssembler Object 
#       7. Finally we have a dataframe with additional columns including 'features' and 'label'
			
########################################################

## 1.0 Call libraries
# Ref :http://spark.apache.org/docs/2.2.0/api/python/pyspark.ml.html#pyspark.ml.Pipeline

# 1.1   For transforming categorical data to integer and to dummy
from pyspark.ml.feature import  StringIndexer, OneHotEncoder

# 1.2   For collecting all features at one place
#       A feature transformer that merges multiple columns into one vector column.
from pyspark.ml.feature import VectorAssembler

# 1.3   To execute all transformation operations as pipeline
from pyspark.ml import Pipeline

# 1.4 Logistic Regression modeling
from pyspark.ml.classification import LogisticRegression

# 1.5 Parameter grid builder and cross-validator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

# 1.6 
from pyspark.ml.evaluation import BinaryClassificationEvaluator



## 2. Read data
#  2.1 Read the 'adult' dataset from spark-sql store
#      And return the data as a spark DataFrame
df = spark.table("adult")

######################### AA. Explore data with Spark DataFrame #########################################

## 3. Spark DataFrame manipulation
#     Ref: https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame

# 3.1 See df's initial column list
cols = df.columns
cols      # We will use this list later; 15 columns (8 + 6 + 1)

# 3.2 DataFrame structure/schema
df.printSchema()
df.dtypes

# 3.3 take() is same as head()
#     Both return a list or Row types
df.head(5)
df.take(5)

# 3.4 Show data in tabular form
df.show()


# 3.5 How many rows
df.count()


# 3.6 Descibe dataframe
df.describe().show()
df.describe(['age', 'workclass']).show()


# 3.7 Show one column
df.select("age").show() 

# OR
df.select(df["age"]).show()

# 3.7.1 Show multiple columns
df.select("age", "workclass").show()

# OR

df.select(df["age"], df["workclass"]).show()  

# 3.7.2 But these do not work:
df.select(df[,["age","workclass"]]).show() 
df.select(df[["age","workclass"]]).show() 


# 3.8 Filter DataFrame with filter command
df.filter(df["age"] > 21).show()   
df.filter(df["age"] == 21).show()   
df.filter(df["marital_status"] == " Never-married").show()   # Note the space
df.filter((df["marital_status"] == " Never-married") & (df["age"] > 21) ).show()   # use '&' for 'and', '|' for 'or', '~' for 'not' 


# 3.9 GroupBy aggregation
df.groupby("age").agg({'capital_gain' : "avg"}).show()
df.groupby("age").agg({'capital_gain' : "avg", 'capital_loss': 'max'}).show()
df.agg({'age' : "max"}).show()



# 3.10 Correlation of two columns
df.corr('age', 'capital_gain')


# 3.11 Return a pandas dataframe
abc = df.toPandas()


######################### BB. Process/Transform Data #########################################

# 2.5 Prepare a list of categorical columns (eight)
x = df.dtypes
categoricalColumns =[]
for i in range(len(x)):
    if(x[i][1] == "string"):
        categoricalColumns.append(x[i][0])


# 2.5.1
categoricalColumns		# List of categorical columns
len(categoricalColumns)		# 8 + 1


# 2.5.2 We will exclude the target, income, from here
categoricalColumns = categoricalColumns[0:8]


# 2.6 An empty 'ToDo' list for feeding into pipeline
# ToDo = [] # ToDo in our Pipeline

# 2.7 Column by column perform transformation to OneHotEncoded form
for cc in categoricalColumns:
  # Create category indexing object for each cat-column 
  stringIndexer = StringIndexer(inputCol= cc, outputCol= cc +"Index")
  model = stringIndexer.fit(df)
  df = model.transform(df)
 
  # Create OneHotEncoder object for each category_indexed column
  #  Just one column is needed/created as it is in sparse-matrix format
  encoder = OneHotEncoder(inputCol= cc + "Index", outputCol= cc + "classVec")
  df = encoder.transform(df)
  

df.dtypes
len(df.columns)

# 2.8 Separately add another stage to convert target, income,
#     into 'label' indices using the StringIndexer
label_To_integer = StringIndexer(inputCol = "income", outputCol = "label")
model = label_To_integer.fit(df)
df = model.transform(df)


# 2.9 Transform all features into a vector using VectorAssembler
numericCols =[]
for i in range(len(x)):
    if(x[i][1] != "string"):
        numericCols.append(x[i][0])
        
numericCols
len(numericCols)	# 6



#       Generate again same names (same as finally given by OneHotEncoder) 
#       for categoricalColumns
# 	  map() function applies (concatenation) function to every member of
#	    an iterable and returns the result.
OneHot = map(lambda c: c + "classVec", categoricalColumns)     # Target not included
OneHot = list(OneHot)
OneHot                                                         # List of names

# Compile list of all OneHot and numerical cols for assembling into 'features'
assemblerInputs = OneHot + numericCols
assemblerInputs
len(assemblerInputs)		# 8 (OneHot) + 6 (numeric) = 14


# Create an object to apply to assemblerInputs, VectorAssembler
#  OutCol is 'always' has a name 'features'. 'features' contains
#   all predictors
assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")
df = assembler.transform(df)


# Examine df
df.take(2)
df.columns
df.dtypes
len(df.columns)     # 18 (one each from ToDo) + 15 (original Cols) = 35


######################### CC.Modeling Data #########################################

# Keep only relevant columns
#  Note now we need just two columns: label and features
#   You can expt by removing others (cols)
selectedcols = ["label", "features"] + cols    # ie 15 + 2 =17 & ignore others
df = df.select(selectedcols)
df.columns

df.select('features','age', 'capital_gain').take(1)
df.select('features').take(1)


### Randomly split data into training and test sets. set seed for reproducibility
(trainingData, testData) = df.randomSplit([0.7, 0.3], seed = 100)
print (trainingData.count())
print (testData.count())



# Create initial LogisticRegression model
lr = LogisticRegression(labelCol="label", featuresCol="features", maxIter=10)
# Train model with Training Data
lrModel = lr.fit(trainingData)


# Make predictions on test data using the transform() method.
# LogisticRegression.transform() will only use the 'features' column.
predictions = lrModel.transform(testData)
predictions.columns         # There is a 'rawPrediction'column also
predictions.printSchema()


# View model's predictions and probabilities of each prediction class
# You can select any columns in the above schema to view as well. For example's sake we will choose age & occupation
selected = predictions.select("label", "prediction", "probability", "age", "occupation", "rawPrediction")
selected.show()


# We can make use of the BinaryClassificationEvaluator method to evaluate our model. 
# The Evaluator expects two input columns: (rawPrediction, label).
#  What is rawPrediction: See : https://stackoverflow.com/questions/37903288/what-do-colum-rawprediction-and-probability-of-dataframe-mean-in-spark-mllib
#    class_k probability: 1/(1 + exp(-rawPrediction_k))
# Evaluate model. Returns AUC
evaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction")
evaluator.evaluate(predictions)

# Note that the default metric for the BinaryClassificationEvaluator is areaUnderROC
evaluator.getMetricName()




# Create ParamGrid for Cross Validation
paramGrid = (ParamGridBuilder()
             .addGrid(lr.regParam, [0.01, 0.5, 2.0])
             .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0])
             .addGrid(lr.maxIter, [1, 5, 10])
             .build())
             
paramGrid = ParamGridBuilder()
paramGrid.addGrid(lr.regParam, [0.01,0.5,2.0]) 
paramGrid.addGrid(lr.elasticNetParam,[0.0,0.5,1.0]).addGrid(lr.maxIter,[1,5,10])
paramGrid.build()  

# Create 5-fold CrossValidator and also specify way to evaluate cv results
cv = CrossValidator(estimator=lr, estimatorParamMaps=paramGrid, evaluator=evaluator, numFolds=5)

# Run cross validations
cvModel = cv.fit(trainingData)
# this will likely take a fair amount of time because of the amount of models that we're creating and testing


# Use test set here so we can measure the accuracy of our model on new data
predictions = cvModel.transform(testData)

# We can also access the model’s feature weights and intercepts easily
print 'Model Intercept: ', cvModel.bestModel.intercept


weights = cvModel.bestModel.coefficients
weights = map(lambda w: (float(w),), weights)  # convert numpy type to float, and to tuple
weightsDF = sqlContext.createDataFrame(weights, ["Feature Weight"])
display(weightsDF)

# View best model's predictions and probabilities of each prediction class
selected = predictions.select("label", "prediction", "probability", "age", "occupation")
display(selected)

       
##########################################################################
# About OneHotEncoder
# A one-hot encoder that maps a column of category indices to a column of binary vectors,
#  with at most a single one-value per row that indicates the input category index. For
#   example with 5 categories, an input value of 2.0 would map to an output vector of 
#   [0.0, 0.0, 1.0, 0.0]. The last category is not included by default (configurable via
#    dropLast) because it makes the vector entries sum up to one, and hence linearly dependent.
#     So an input value of 4.0 maps to [0.0, 0.0, 0.0, 0.0].
# Note
#  This is different from scikit-learn’s OneHotEncoder, which keeps all categories. The output 
#    vectors are sparse.               
##########################################################################



