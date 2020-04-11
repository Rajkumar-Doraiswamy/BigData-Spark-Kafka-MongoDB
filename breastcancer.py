##***********************************************************************
## 12th Jan, 2018
## My folder: /home/ashok/Documents/spark/breast_cancer
#
# A. Problem: Predicting breast cancer
# B. Objectives:
#            a. Making predictions when no of ones are much less than no of zeros
#               that is predicting outliers
#            b. Using logistic modeling with binomial family
#            c. Using pyspark for modeling
#
# Convert existing data file to libsvm format
#            a. Columns such as id etc should be pre-removed
#            a. All fields must be numeric or coded as integers
#            b. ./csv2libsvm.py   -l 0 -s  data_modified.csv data_modified.txt
#               where 0 stands for 1st column, the target. And -s means skip headers
#		libsvm format does not have headers only column numbers
#
# Make hdfs directory and transfer data files as:
#       $ hdfs dfs -rm -r /user/ashok/data_files/bc
# 	$ hdfs dfs -mkdir -p /user/ashok/data_files/bc
#       $ hdfs dfs -put  /home/ashok/Documents/spark/breast_cancer/data_modified.txt  /user/ashok/data_files/bc

#
# References:
#  1. https://spark.apache.org/docs/2.2.0/ml-classification-regression.html#gradient-boosted-tree-classifier
#  2. https://stackoverflow.com/questions/39535447/attributeerror-dataframe-object-has-no-attribute-map/39536218
#  3. https://github.com/zygmuntz/phraug2/blob/master/csv2libsvm.py

## 
## Data is in folder:   /cdata/breast_cancer
#         OR in hdfs:	/user/ashok/data_files/bc


# 1.0
# Start pyspark

# 2.0
from pyspark.ml.classification import LogisticRegression

# 3.0 Load libsvm training data
training = spark.read.format("libsvm").load("hdfs://localhost:9000/user/ashok/data_files/bc/data_modified.txt")

# 4.0
lr = LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)


# 5.0 Fit the model
lrModel = lr.fit(training)

# 6.0 Print the coefficients and intercept for logistic regression
print("Coefficients: " + str(lrModel.coefficients))
print("Intercept: " + str(lrModel.intercept))

# 7.0 We can also use the multinomial family for binary classification
mlr = LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8, family="multinomial")

# 7.1 Fit the model
mlrModel = mlr.fit(training)

# 7.2 Print the coefficients and intercepts for logistic regression with multinomial family
print("Multinomial coefficients: " + str(mlrModel.coefficientMatrix))
print("Multinomial intercepts: " + str(mlrModel.interceptVector))

# ================================================================
# TRASH for later reference






# 3.1 Read car_data file
data = spark.read.csv("hdfs://localhost:9000/user/ashok/data_files/bc/data_modified.csv", header=True, mode="DROPMALFORMED")



data = spark.read.option("inferSchema", "true").option("header", "true").csv("hdfs://localhost:9000/user/ashok/data_files/bc/data_modified.csv")
type(data)
data.take(3)

# 3.2 What is type of car. car is a SparkDataFrame
class(car)

# 3.3 check car structure
str(car)

# 3.4 Dimensions
dim(car)

## 4
## B. RANDOMIZING ROWS
# Take as many samples from data as there are rows
#   Default: Sample is taken without replacement
#     That is, once a sample record is picked up, it is no-longer
#        available for repeat selection
#           Output is full dataframe and not just row numbers

# Split data approximately into training (60%) and test (40%)
    training, test = data.randomSplit([0.6, 0.4])

training.count()
test.count()


vectorAssembler = VectorAssembler().setInputCols(["diagnosis","radius_mean","texture_mean","perimeter_mean","area_mean","smoothness_mean","compactness_mean","concavity_mean","concave_points_mean","symmetry_mean","fractal_dimension_mean","radius_se","texture_se","perimeter_se","area_se","smoothness_se","compactness_se","concavity_se","concave_points_se","symmetry_se","fractal_dimension_se","radius_worst","texture_worst","perimeter_worst","area_worst","smoothness_worst","compactness_worst","concavity_worst","concave_points_worst","symmetry_worst","fractal_dimension_worst"]).setOutputCol("features")

transformationPipeline = Pipeline().setStages([vectorAssembler])
fittedPipeline = transformationPipeline.fit(training)
transformedTraining = fittedPipeline.transform(training)







kmeans = KMeans().setK(2).setSeed(1)
kmModel = kmeans.fit(transformedTraining)

kmModel.computeCost(transformedTraining)


## 4.1 SUBDIVIDE DATA INTO TRAINING AND VALIDATION
#         Subdivide data into training and validation parts

# 4,2
dim(train)
dim(valid)

# 4.3
# cache(train)

## 5
# Create binomial family model with glm
#  Takes time. Some warnings appear...Do not worry....
model <- spark.glm(class_value ~ . , data = train, family = "binomial")
model = GradientBoostedTrees.trainClassifier(training, categoricalFeaturesInfo={}, numIterations=3)

# 5.1
summary(model)

# 5.2 Make predictions. col 7 'valid' is class_value; not needed for modeling
predictions <- predict(model, newData = valid[,-c(7)] )
class(predictions)

# 5.3
showDF(predictions)

# 5.4 select just what is to be predicted
modelPrediction <- SparkR::select(predictions,  "prediction")

# 5.5 Few rows of data
head(modelPrediction)

# 5.6 Convert result to R data frame
df<-as.data.frame(modelPrediction)
df$prediction<-round(df$prediction)   # Round to 0 or 1

# 5.7 Add to it a column of actual values of class_value
df$actual<-head(SparkR::select(valid,valid$class_value),nrow(valid))
head(df)

# 5.8 Check validation accuracy
#   Compare each value of predicted (p) against actual value in validy
#     Sum up all such instances. The sum divided by total validation data
#        provides accuracy measure
#          (Note that sum(TRUE) = 1 and sum(TRUE + TRUE) = 2 )
sum(df$prediction == df$actual) / nrow(df)

## 6  Stop sparkR session
sparkR.session.stop() 

# 7. Quit R
q()


######################## FINISH #############################

## 8. Quick steps in NaiveBayes modeling
modelNB <- spark.naiveBayes(class_value ~ . , data = train)
summary(modelNB)
predictionsNB <- predict(modelNB, newData = valid[,-c(7)] )
class(predictions)
showDF(predictionsNB)
modelPredictionNB <- SparkR::select(predictionsNB,  "prediction")
head(modelPredictionNB)
dfNB<-as.data.frame(modelPredictionNB)
dfNB$actual<-head(SparkR::select(valid,valid$class_value),nrow(valid))
head(df)
sum(dfNB$prediction == dfNB$actual) / nrow(dfNB)

################################################################
 Convert csv to libsvm format (for Spark)
Python script here:
https://github.com/zygmuntz/phraug/blob/master/csv2libsvm.py

How to use it:
prepare the input csv as follows:
- remove non features (like row number or similar id column which brings no information)
- transform target (or class) from string to numeric
- then output the new train file as train-for-libsvm.csv
- note which column number is the target
- then run ms-dos command (on windows):

Example of script in R to modify the input csv:
train <- read.csv('train.csv')
train$target <- as.numeric(train$target)
# remove id
train$id = NULL
write.csv(train, "train_numeric_targets.csv", quote=FALSE,row.names=FALSE)



csv2libsvm.py train-for-libsvm.csv train-libsvm.txt 93 1

===========================
parsedData = data.map(lambda line: array([float(x) for x in line.split(' ')]))
clusters = KMeans.train(training, 2, maxIterations=10, initializationMode="random")

