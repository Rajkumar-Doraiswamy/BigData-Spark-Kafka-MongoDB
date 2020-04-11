## Last amended: 15th Jan,2018
## Myfolder: /home/ashok/Documents/spark/adult
## Datasource: Adult dataset from UCI
##             https://archive.ics.uci.edu/ml/machine-learning-databases/adult/       => Data source

## Problem: Predict income of a person from his census data
#  Ref: https://docs.databricks.com/spark/latest/mllib/binary-classification-mllib-pipelines.html## Objectives:
##		1. Learn ML on Spark
##		2. Learn importing data files in spark
##		3. Learn data preprocessing on spark
##		4. Build ML models

## Start hadoop and then pyspark:
##		$ cd ~/
##		$ pyspark
########################################################

## 1.0 Call librraies
# Ref :http://spark.apache.org/docs/2.2.0/api/python/pyspark.ml.html#pyspark.ml.Pipeline

# 1.1   For transforming categorical data to dummy
from pyspark.ml.feature import OneHotEncoder, StringIndexer

# 1.2   For collecting all features at one place
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
dataset = spark.table("adultdata.adult")

# 2.2 See dataset's columns
cols = dataset.columns
cols

# 2.3 take() is same as head()
dataset.take(10)

# 2.4
dataset.show()

# 2.5 Prepare a list of categorical columns (eight)
categoricalColumns = ["workclass", "education", "marital_status", "occupation", "relationship", "race", "sex", "native_country"]

# 2.6 An empty 'stages' list for our feedinginto pipeline
stages = [] # stages in our Pipeline

# 2.7 Prepare pipeline stage, column by colyumn
for categoricalCol in categoricalColumns:
  # Create category indexing object for each cat-column 
  stringIndexer = StringIndexer(inputCol=categoricalCol, outputCol=categoricalCol+"Index")
  
  # Create OneHotEncoder object for each category_indexed column
  encoder = OneHotEncoder(inputCol=categoricalCol+"Index", outputCol=categoricalCol+"classVec")
  
  # Add both objects to a list. 'stages' is a list of lists.
  #  These are not run here, but will run all at once later on.
  stages += [stringIndexer, encoder]

# 2.7.1
stages

# 2.8 Separately add another stage to convert target, income, into 'label' indices using the StringIndexer
label_stringIdx = StringIndexer(inputCol = "income", outputCol = "label")
stages += [label_stringIdx]   # This stage does not have onehotcoder transformation


# 2.9 Transform all features into a vector using VectorAssembler
numericCols = ["age", "fnlwgt", "education_num", "capital_gain", "capital_loss", "hours_per_week"]
# map() function applies a function to every member of an iterable and returns the result.
mp = map(lambda c: c + "classVec", categoricalColumns)
mp = list(mp)
assemblerInputs = mp + numericCols
assemblerInputs = list(map(lambda c: c + "classVec", categoricalColumns)) + numericCols

assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")
stages += [assembler]

# Create a Pipeline.
pipeline = Pipeline(stages=stages)
# Run the feature transformations.
#  - fit() computes feature statistics as needed.
#  - transform() actually transforms the features.
pipelineModel = pipeline.fit(dataset)
dataset = pipelineModel.transform(dataset)
dataset.take(2)

# Keep relevant columns
selectedcols = ["label", "features"] + cols
dataset = dataset.select(selectedcols)
display(dataset)

### Randomly split data into training and test sets. set seed for reproducibility
(trainingData, testData) = dataset.randomSplit([0.7, 0.3], seed = 100)
print (trainingData.count())
print (testData.count())



# Create initial LogisticRegression model
lr = LogisticRegression(labelCol="label", featuresCol="features", maxIter=10)
# Train model with Training Data
lrModel = lr.fit(trainingData)

# Make predictions on test data using the transform() method.
# LogisticRegression.transform() will only use the 'features' column.
predictions = lrModel.transform(testData)
predictions.columns
predictions.printSchema()


# View model's predictions and probabilities of each prediction class
# You can select any columns in the above schema to view as well. For example's sake we will choose age & occupation
selected = predictions.select("label", "prediction", "probability", "age", "occupation")
display(selected)


# We can make use of the BinaryClassificationEvaluator method to evaluate our model. 
# The Evaluator expects two input columns: (rawPrediction, label).


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

              



