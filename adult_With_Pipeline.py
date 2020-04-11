## Last amended: 14/10/2018
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

		$ cd ~
		$ pyspark
		OR as:
		$ pyspark --driver-memory 2g     for those having more memory

# pyspark configuration is avaialble at:

	 http://localhost:4040

	 Check for parameters such as:
	 	spark.master
	 	spark.driver.host
	 	spark.driver.memory

	In local mode there is one jvm for both excutors and drivers



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
#			Instantiate a StringIndexer (converts to integers)
#				Sample: StringIndexer(inputCol= cc, outputCol= cc +"Index")		=> One column created
#			Instantiate a OneHotEncoder object (converts to sparse vector)
#				Sample: OneHotEncoder(inputCol= cc+"Index", outputCol=cc+"classVec")	=> One more column created
#			Append these two objects to a 'ToDo' list
#		next column
#	3. Target column is also categorical. But only StringIndex it
#	   and do not convert to OneHotEncoder format
#	4. Add the StringIndexer object from last step also to 'ToDo' object
#	5. Extract a list of numeric columns
#	6. Assemble a VectorAssembler object, using:
#		a. List of numericcols
#		b. List of OneHotEncoded columns
#		c. a+b, ie predictor columns => features
#			Sample: VectorAssembler(inputCols=assemblerInputs, outputCol="features")
#	7. Append VectorAssembler Object also to 'ToDo' list
#	8. Create a pipeline object consisting of all 'ToDo's: Pipeline(stages=ToDo)
#	9. Fit and transform Pipeline object on our DataFrame
#       10.Pipeline execution results in a dataframe with additional columns including 'features' and 'label'

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

# 1.7 Misc
import time


## 2. Read data
#  2.1 Read the 'adult' dataset from spark-sql store ('adultdata' database)
#      And return the data as a spark DataFrame

df = spark.table("adultdata.adult")

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

df.select("age").show()     # Not as: df["age"]

# OR

df.select(df["age"]).show()

# 3.7.1 Show multiple columns

df.select("age", "workclass").show()

# OR

df.select(df["age"], df["workclass"]).show()

# 3.7.2 But these do not work:
# df.select(df[,["age","workclass"]]).show()
# df.select(df[["age","workclass"]]).show()


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


######################### BB. Process Data using PipeLine #########################################

# 4.1 Prepare a list of categorical columns (eight)

x = df.dtypes
# 4.2
categoricalColumns =[]
# 4.3
for i in range(len(x)):
    if(x[i][1] == "string"):
        categoricalColumns.append(x[i][0])


# 4.4

categoricalColumns		# List of categorical columns
len(categoricalColumns)		# 8 + 1


# 4.5 We will exclude the target, income, from here

categoricalColumns = categoricalColumns[0:8]


# 4.5.1 Transform just one column manually
##      StringIndexer is same as LabelEncoder of sklearn

si = StringIndexer(inputCol = "workclass", outputCol = "workclass" + "Index")
model = si.fit(df)
dfa = model.transform(df)
dfa.columns

# 4.5.2 So how is our new column"

dfa.select('workclassIndex').show()


# 4.5.3 Further transform it to onehot encoded form

ohe = OneHotEncoder(
                    inputCol = "workclassIndex",
                    outputCol = "workclassclassvec"
                   )

# 4.5.4

dfa = ohe.transform(dfa)
dfa.columns
dfa.select('workclassclassvec').show()


## 4.5.5 Data normalization. How?
##       What is you would limt to normalize: age, capital_gain, capital_loss etc
##       Here is the way
#   from pyspark.ml.feature import MinMaxScaler
#   maxage = df.agg({"age": "max"}).collect()[0][0]
#   minage = df.agg({"age": "min"}).collect()[0][0]
#   df = df.withColumn('age_normalise', (df.age - minage)/(maxage - minage))



# 4.6 An empty 'ToDo' list for feeding into pipeline

ToDo = [] # ToDo in our Pipeline

# categoricalColumns = categoricalColumns[1:]

# 4.7 Prepare pipeline stage, column by column ie for every column
#     ToDo = [s_col1,o_col1,s_col2,o_col2...]

for cc in categoricalColumns:
  # 4.7.1 Create category indexing object for each cat-column
  stringIndexer = StringIndexer(inputCol= cc, outputCol= cc +"Index")

  # 4.7.2 Create OneHotEncoder object for each category_indexed column
  encoder = OneHotEncoder(inputCol= cc + "Index", outputCol= cc + "classVec")

  # 4.7.3 Add both objects to a list. 'ToDo' is a list of lists.
  ToDo = ToDo + [stringIndexer, encoder]    # + is same as ToDo.append()


# 4.8

ToDo		# 2 X (no of categorical columns) ie
len(ToDo)	# 2 X 8 = 16


# 5.0 Separately add another stage to convert target, income,
#     into 'label' indices using the StringIndexer

label_To_integer = StringIndexer(inputCol = "income", outputCol = "label")
ToDo.append(label_To_integer)   # This stage does not have onehotcoder transformation
len(ToDo)	# 16 + 1


# 5.1 Get remaining list of numerical columns

numericCols = [ i[0]   for i in df.dtypes  if i[1] != "string" ]


# 5.2

numericCols
len(numericCols)	# 6

## 6 So total features are:
##    i)   8 onehotencoded
##   ii)   1 target
##  iii)   6 numerical columns
##  Total  15


# 7   We will bundle all predictor features under one column using
#     VectorAssembler.

"""
Ref: https://spark.apache.org/docs/latest/ml-features.html#vectorassembler
VectorAssembler

VectorAssembler is a transformer that combines a given
list of columns into a single vector column. It is useful
for combining raw features and features generated by
different feature transformers into a single feature vector,
in order to train ML models like logistic regression and
decision trees. VectorAssembler accepts the following input
column types: all numeric types, boolean type, and vector type.
In each row, the values of the input columns will be
concatenated into a vector in the specified order.

Examples

Assume that we have a DataFrame with the columns
id, hour, mobile, userFeatures, and clicked:

 id | hour | mobile | userFeatures     | clicked
----|------|--------|------------------|---------
 0  | 18   | 1.0    | [0.0, 10.0, 0.5] | 1.0



userFeatures is a vector column that contains three user features.
We want to combine hour, mobile, and userFeatures into a single feature vector
called features and use it to predict clicked or not. If we set
VectorAssembler’s input columns to hour, mobile, and userFeatures and
output column to features, after transformation we should get the
following DataFrame:

 id | hour | mobile | userFeatures     | clicked | features
----|------|--------|------------------|---------|-----------------------------
 0  | 18   | 1.0    | [0.0, 10.0, 0.5] | 1.0     | [18.0, 1.0, 0.0, 10.0, 0.5]

"""


# 5.3  Generate again exactly same names
#      (same as finally given by OneHotEncoder) for categoricalColumns.
# 	  map() function applies (concatenation) function to every member of
#	    an iterable and returns the result.

#OneHotNames = map(lambda c: c + "classVec", categoricalColumns)     # Target not included
# 5.3.1

#OneHotNames = list(OneHotNames)
# 5.3.2

#OneHotNames                                        # List of names: 8

## OR, write manually,

# 5.3.3 Considering #4.7.2 above, I can also write manually as:

OneHotNames = ['workclassclassVec','educationclassVec','marital_statusclassVec',
               'occupationclassVec','relationshipclassVec','raceclassVec',
               'sexclassVec','native_countryclassVec']


# 6. Combined list of all OneHot and numerical cols for VectorAssembling
#    into 'features'

assemblerInputs = OneHotNames + numericCols

# 6.1

assemblerInputs
len(assemblerInputs)		# 8 (OneHot) + 6 (numeric) = 14


# 7. Create a VectorAssembler object to apply dataframe
#    OutCol always has a name 'features'. 'features' contains
#     list of values from all predictors

assembler = VectorAssembler(
                           inputCols=assemblerInputs,
                           outputCol="features"
                           )
# 7.1

assembler

# 7.2 Append this job to pipeline stages also

# 7.2.1   Previous list of jobs

ToDo

# 7.2.2

ToDo.append(assembler)

# 7.2.3  Current list

ToDo
len(ToDo)           # (8 StringIndexer + 8 OneHotEncoder + 1 Target col + 1 VectorAssembler) = 18

# 8. Create a Pipeline.

pipeline = Pipeline(stages=ToDo)

# 8.1 Run the feature transformations.
#  - fit() computes feature statistics as needed (NOTE: 'as needed')
#  - transform() actually transforms the features.

pipelineModel = pipeline.fit(df)

# 8.2

df = pipelineModel.transform(df)

# 8.3 Examine df

df.take(2)
df.select('features').take(1)
df.columns
df.dtypes
len(df.columns)     # 18 (one each from ToDo) + 15 (original Cols) = 33


######################### CC.Modeling Data #########################################

# 9. Keep only relevant columns
#    Note now we need just two columns: label and features
#      You can expt below by removing other cols (cols)
#       Variable 'cols' is from para #3.1 above

selectedcols = ["label", "features"] + cols    # ie 15 + 2 =17 & ignore others
# 9.1

df = df.select(selectedcols)
# 9.2

df.columns

# 9.3

df.select('features','age', 'capital_gain').take(1)
# 9.4

df.select('features').take(1)


# 10. Randomly split data into training and test sets. set seed for reproducibility

(trainingData, testData) = df.randomSplit([0.7, 0.3], seed = 100)

# 10.1

trainingData.count()
testData.count()


# 11.  Create initial LogisticRegression model
# Ref: https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.classification.LogisticRegression

start = time.time()
lr = LogisticRegression(
                        labelCol="label",
                        featuresCol="features",
                        maxIter=10
                        )

# 11.1 Train model with Training Data

lrModel = lr.fit(trainingData)
end = time.time()
(end-start)/60


# 12. Make predictions on test data using the transform() method.
#     LogisticRegression.transform() will only use the 'features' column.

predictions = lrModel.transform(testData)
predictions.columns         # There is a 'rawPrediction'column also
predictions.printSchema()


# 13. View model's predictions and probabilities of each prediction class
#     You can select any columns in the above schema to view as well.
#     For example's sake we will choose age & occupation
# 13.1  What is rawPrediction:
#       See : https://stackoverflow.com/questions/37903288/what-do-colum-rawprediction-and-probability-of-dataframe-mean-in-spark-mllib
#       class_k probability: 1/(1 + exp(-rawPrediction_k))

selected = predictions.select("label", "prediction", "probability", "age", "occupation", "rawPrediction")

# 13.1

selected.show()


# 14. We can make use of the BinaryClassificationEvaluator method to
#     evaluate our model.
#     The Evaluator expects two input columns: (rawPrediction, label)
#     and a value of 'metricName'
#     By default -label- parameter has value 'label', 'metricName'
#     has value of "areaUnderROC"


# 14.2 Evaluate model. Returns AUC

evaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction")
evaluator.evaluate(predictions)

# 14.3 Note that the default metric for the
#      BinaryClassificationEvaluator is areaUnderROC

evaluator.getMetricName()


######################### CC.GridSearch based Model Selection #########################################

# 15. Create ParamGrid for Cross Validation
#     Ref: https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.tuning.ParamGridBuilder
#     Ref: https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.tuning.CrossValidator

#  K-fold cross validation performs model selection by splitting the dataset into a set of
#    non-overlapping randomly partitioned folds which are used as separate training and
#     test datasets e.g., with k=3 folds, K-fold cross validation will generate 3 (training, test)
#      dataset pairs, each of which uses 2/3 of the data for training and 1/3 for testing.
#       Each fold is used as the test set exactly once.

# 15.1 One Way

grid = (ParamGridBuilder()
             .addGrid(lr.regParam, [0.01, 0.5, 2.0])		# Regularization (L2) parameter value
             .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0])	# alpha. aplha =0 => only L2, alpha =1 => Only L1
             .addGrid(lr.maxIter, [1, 5, 10])			# Max iteration
             .build())

# 15.2 Another way (This way creates problems)

grid= ParamGridBuilder()
grid.addGrid(lr.regParam, [0.01,0.5,2.0])
grid.addGrid(lr.elasticNetParam,[0.0,0.5,1.0]).addGrid(lr.maxIter,[1,5,10])
grid.build()

# 15.3 Create 5-fold CrossValidator-object
#      Ref: https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.tuning.CrossValidator

# 15.3.1 Specify first way to evaluate cv results

lr = LogisticRegression(labelCol="label",      \
                        featuresCol="features" \
                        )

evaluator = BinaryClassificationEvaluator()
# 15.3.2

cv = CrossValidator(estimator=lr, \
                    estimatorParamMaps= grid,     \
                    evaluator=evaluator,  \
                    numFolds=5
                    )


# 15.4
# Run cross validations
#  this will likely take around 20 minutes

start = time.time()
cvModel = cv.fit(trainingData)
end = time.time()
print (((end-start)/60) , "minutes")

# 15.5 Best performance metrics

cvModel.avgMetrics[0]

# 15.6 Best Model

bestModel = cvModel.bestModel

# 15.7 Make predictions on test set here to evaluate accuracy of our model

predictions = bestModel.transform(testData)

# 15.8 View best model's predictions and probabilities of each prediction class

selected = predictions.select("label", "prediction", "probability", "age", "occupation", "rawPrediction")
selected.show()

# 15.9 Evaluate predictions

evaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction")
evaluator.evaluate(predictions)


# 15.10 We can also access the model’s feature weights and intercepts easily

print ('Model Intercept: ', cvModel.bestModel.intercept)
weights = bestModel.coefficients
weights = list(map(lambda w: (float(w),), weights))  # convert numpy type to float, and to tuple

sqlContext = SQLContext(sc)
weightsDF = sqlContext.createDataFrame(weights, ["Feature Weight"])
weightsDF

##########################################################################
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

# About VectorAssembler
# A one-hot encoder that maps a column of category indices to a column of binary vectors,
#  with at most a single one-value per row that indicates the input category index. For
#   example with 5 categories, an input value of 2.0 would map to an output vector of
#   [0.0, 0.0, 1.0, 0.0]. The last category is not included by default (configurable via
#    dropLast) because it makes the vector entries sum up to one, and hence linearly dependent.
#     So an input value of 4.0 maps to [0.0, 0.0, 0.0, 0.0].
# Note
#  This is different from scikit-learn’s OneHotEncoder, which keeps all categories. The output
#    vectors are sparse.
