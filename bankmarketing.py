# Last amended: 11th October, 2018
# My folder: /home/ashok/Documents/spark/bike_sharing_dataset


# Ref: 
# 1. https://github.com/susanli2016/PySpark-and-MLlib/blob/master/Machine%20Learning%20PySpark%20and%20MLlib.ipynb

#  2.  https://towardsdatascience.com/machine-learning-with-pyspark-and-mllib-solving-a-binary-classification-problem-96396065d2aa

# 3. https://spark.apache.org/docs/2.1.0/ml-classification-regression.html#classification-and-regression

# 4. https://github.com/susanli2016/PySpark-and-MLlib     <==Good site

"""
Analyzing a bankmarketing data set
================================
Objectives
---------------
	i)     Binary predictions using Logistic Regression
	ii)    Binary predictions using Decision Trees
	iii)  Binary predictions using Random Forest
	iv)  Binary predictions using Gradient Boosting
	v)  Binary predictions using Cross-vaidation
	vi) In all above cases we build pipeline of actions


Data:
-----
Kaggle: https://www.kaggle.com/rouseguy/bankbalanced


"""

# 1.0 Start hadoop

$ ./allstart.sh

# 1.1 Transfer datafiles from local filesystem to hadoop filesystem

$ hdfs dfs -put /cdata/bankmarketing/bank.csv hdfs://localhost:9000/user/ashok/data_files

$ hdfs dfs -ls  hdfs://localhost:9000/user/ashok/data_files/bank*


# 1.2 Start pyspark

$ pyspark



# 1.3 Import libraries 
# 1.3.1 For converting categorical variables to integers
from pyspark.ml.feature import  StringIndexer

# 1.3.2 For coding integers to one hot codes
from pyspark.ml.feature import OneHotEncoderEstimator

# 1.3.3 For assembling all features as one column
from pyspark.ml.feature import  VectorAssembler

# 1.3.4  To create pipeline of tasks:
from pyspark.ml import Pipeline

# 1.3.5 Modeling libraries:
# Ref: https://spark.apache.org/docs/2.1.0/ml-classification-regression.html#binomial-logistic-regression
from pyspark.ml.classification import LogisticRegression

# 1.3.6 Import prediction evaluator
from pyspark.ml.evaluation import BinaryClassificationEvaluator

# 1.3.6 Misc
import time

# 2. 0 Load and understand the data

# 2.0.1
URL_of_file = "hdfs://localhost:9000/user/ashok/data_files/"

# 2.0.2 
df =     spark.read.csv( URL_of_file  + "bank.csv" ,
                                                inferSchema = True, 
                                                header = True
                                               )


##################### AA. Explore data #######################

# 2.1 Cache the data so that we only read it from disk once:

df.cache()

# 2.2 See data

df.show(3)

# 2.3 How many rows?

df.count()

# 3. Get column names and structure:
# 3.1  Our target class is 'deposit'

df.columns
df.printSchema()

# 3.2  Also day and month columns are not really useful, 
#         we will drop these two columns. Either use drop()

# df = df.drop("day").drop("month")

# OR, just select useful cols:

df = df.select('age', 'job', 'marital', 'education', 'default',
                              'balance', 'housing', 'loan', 'contact', 'duration',
                               'campaign', 'pdays', 'previous', 'poutcome', 'deposit')

# 3.2.1
cols = df.columns

# 3.2.2 Just verify schema again:

df.printSchema()


##################### BB. Transform Data ####################

# 4.0 Proceed with transformations:

# 4.1 List of our categorical columns. Leave out deposit. Why?
#        We will transform all these columns but 'deposit' to OHE

catCols = ['job', 'marital', 'education', 'default',
                      'housing', 'loan', 'contact', 'poutcome']

# 4.2 Initialise pipeline stages:

stages = []

# 4.3 Loop through each categorical column to create
#        transformational objects:

for c in catCols:

    # For every cat columns, Instantiate a new object,
    strIndexer = StringIndexer(inputCol = c, outputCol = c + 'Index')

    ohe  = OneHotEncoderEstimator(
                            inputCols=[c+ 'Index'],
                           outputCols=[c + "classVec"]
                           )

    stages += [strIndexer, ohe]

stages

# 4.4  col 'deposit' must also be transformed to integers:

label_strIdx = StringIndexer(inputCol = 'deposit', outputCol = 'label')
stages += [label_strIdx]

# 4.5 Our numerical columns:
numericCols = ['age', 'balance', 'duration',
                                 'campaign', 'pdays', 'previous']

# 5. Finally assemble all inputs in as one feature:

# 5.1 List of features to be assembled at one place:
assemblerInputs = [c + "classVec" for c in catCols] + numericCols

assemblerInputs

# 5.2 Instantiate assembler object:

assembler = VectorAssembler(
                                                 inputCols=assemblerInputs,
                                                 outputCol="features"
                                                                    )

# 5.3 Add it as a final stage of transformational pipeline

stages += [assembler]

stages

# 6. Create pipeline object:

pipeline = Pipeline(stages = stages)

# 6.1  Train piepline on dataset
pipelineModel = pipeline.fit(df)

# 6.2 Tranform dataset:
df = pipelineModel.transform(df)

# 6.3 See the last transformed col from dataset:
df.select('label','features').show(3)

# 6.4 There is a long list of newly created columns:
#         during pipeline transformation. Remember
#        we had inputCol, outputCol for each categorical
#       column (See the for-loop above):

df.columns

# 6.5 So we select just the original and the two more columns.
#        Leave out others.
selectedCols = ['label', 'features'] + cols

len(selectedCols)

# 6.6 Next, schema:
df = df.select(selectedCols)
df.printSchema()


################### CC. Logictic Regression ####################


# 6.6  Split dataset:
train, test = df.randomSplit([0.7, 0.3], seed = 2018)

# 6.6.1
train.count()
test.count()

# 7.0 Perform modeling:

# 7.1 Create modeling object first:

lr = LogisticRegression(
                                                  featuresCol = 'features',
                                                  labelCol = 'label',
                                                  maxIter=10
                                                  )

# 7.2 Train the modeling object & get model:

start = time.time()
lrModel = lr.fit(train)
end = time.time()
tt = (end - start)/60
tt                                           # 1 minute

# 7.3 Get some details of model:

# 7.3.1 Extract the summary from the
#            returned LogisticRegressionModel instance
#            trained in this example

trainingSummary = lrModel.summary

# 7.3.2 Obtain the ROC and areaUnderROC.
trainingSummary.areaUnderROC              # 88.42%


# 8 Make predictions:
predictions = lrModel.transform(test)


# 8.1 Display some columns:
predictions.select('age', 'job', 'label', 'rawPrediction',
                                         'prediction', 'probability').show(10)



# 9 Evaluate model using AUC:
evaluator = BinaryClassificationEvaluator()
evaluator.evaluate(predictions)    		# 88.76%



################### DD. Decision Tree ####################

# 10.0 Call library
from pyspark.ml.classification import DecisionTreeClassifier

# 10.1 Instantiate modeling object:

dt = DecisionTreeClassifier(featuresCol = 'features',
                                                            labelCol = 'label', 
                                                            maxDepth = 3
                                                            )

# 10.2 Fit/train model
dtModel = dt.fit(train)

# 10.3 MAke predictions and evaluate
predictions = dtModel.transform(test)
predictions.select('age', 'job', 'label', 'rawPrediction', 'prediction', 'probability').show(10)

# 10.4
evaluator = BinaryClassificationEvaluator()
evaluator.evaluate(predictions)

################ EE. Random Forest Classification ##################

# 11.0 Import library
from pyspark.ml.classification import RandomForestClassifier

# 11.1 Instantiate object and fit:

rf = RandomForestClassifier(
                                                               featuresCol = 'features',
                                                               labelCol = 'label'
                                                               )
# 11.2
rfModel = rf.fit(train)

# 11.3 Make predictions:
predictions = rfModel.transform(test)

predictions.select('age', 'job', 'label', 'rawPrediction',
                                         'prediction', 'probability').show(10)

# 11.4 Evaluate:
evaluator = BinaryClassificationEvaluator()
evaluator.evaluate(predictions)


################ FF. Gradient Boosting Classification ##################

# 12. All steps
from pyspark.ml.classification import GBTClassifier
# 12.1
gbt = GBTClassifier(maxIter=10)
gbtModel = gbt.fit(train)
# 12.2
predictions = gbtModel.transform(test)
predictions.select('age', 'job', 'label', 'rawPrediction',
                                        'prediction', 'probability').show(10)

# 12.3
evaluator = BinaryClassificationEvaluator()
evaluator.evaluate(predictions)

# 12.4
gbt.explainParams()


############# GG. Gradient Boosting Cross-validation ##################

# 12.5 Cross validation using parameter grid
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

# 12.6
paramGrid = (ParamGridBuilder()
             .addGrid(gbt.maxDepth, [2, 4, 6])
             .addGrid(gbt.maxBins, [20, 60])
             .addGrid(gbt.maxIter, [10, 20])
             .build())

# 12.7
cv = CrossValidator(estimator=gbt,
                                            estimatorParamMaps=paramGrid,
                                           evaluator=evaluator,
                                           numFolds=5
                                            )

# 12.8 Run cross validations. 
#           Takes about 6 minutes as it is training over 20 trees!
cvModel = cv.fit(train)
predictions = cvModel.transform(test)
evaluator.evaluate(predictions)







########################### FINISH #######################
