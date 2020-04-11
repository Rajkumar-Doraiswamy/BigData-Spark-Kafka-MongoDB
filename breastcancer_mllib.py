##***********************************************************************
## 03/02/2018
## My folder: /home/ashok/Documents/spark/breast_cancer
#
# A. Problem: Predicting breast cancer
# B. Objectives:
#            a. Using random forest modeling for classification
#            b. Using pyspark for modeling
#
# C. Convert existing data file to libsvm format
# ===========================================
#     ==>>   a. See folder 'spark/libsvm_hadoop_streaming' for a better example on hadoop     <<==
#            b. Columns such as id etc should be pre-removed
#            c. All fields must be numeric or coded as integers
#            d. ./csv2libsvm.py   -l 0 -s  data_modified.csv data_modified.txt
#               where 0 stands for 1st column, the target. And -s means skip headers
#		libsvm format does not have headers only column numbers
#
# D. Make hdfs directory and transfer data files as:
#       $ hdfs dfs -rm -r /user/ashok/data_files/bc
# 	$ hdfs dfs -mkdir -p /user/ashok/data_files/bc
#       $ hdfs dfs -put  /home/ashok/Documents/spark/breast_cancer/data_libsvm.txt  /user/ashok/data_files/bc
#       $ hdfs dfs -ls /user/ashok/data_files/bc

#
# E. References:
#  1. RandomForest:  https://spark.apache.org/docs/2.2.0/mllib-ensembles.html    
#  2. MLlib: http://spark.apache.org/docs/latest/api/python/pyspark.mllib.html#module-pyspark.mllib.util
#  3. https://stackoverflow.com/questions/39535447/attributeerror-dataframe-object-has-no-attribute-map/39536218
#  4. libsvm: https://github.com/zygmuntz/phraug2/blob/master/csv2libsvm.py
#  5. RDD operations: https://www.codementor.io/jadianes/spark-python-rdd-basics-du107x2ra
#                     https://spark.apache.org/docs/2.2.0/api/python/pyspark.html#pyspark.RDD

## 
## Data is in folder:   /cdata/breast_cancer
#         OR in hdfs:	/user/ashok/data_files/bc


# 1.0
# Start first hadoop and then pyspark, as:

	$ cd ~
	$ pyspark
	OR as:
	$ pyspark --driver-memory 2g     for those having more memory

# 1.1 pyspark configuration is avaialble at:

	 http://localhost:4040	
	 
	 Check for parameters such as:
	 	spark.master
	 	spark.driver.host
	 	spark.driver.memory

	In local mode there is one jvm for both excutors and drivers	 	
	 
	
	
# 2.0 Call libraries
from pyspark.mllib.tree import RandomForest, RandomForestModel
from pyspark.mllib.evaluation import BinaryClassificationMetrics
from pyspark.mllib.util import MLUtils

# 3.0 Load and parse the data file into an RDD of LabeledPoint.
data = MLUtils.loadLibSVMFile(sc, "hdfs://localhost:9000/user/ashok/data_files/bc/data_libsvm.txt")

# Python spark RDD operations
# Ref:  https://spark.apache.org/docs/2.2.0/api/python/pyspark.html#pyspark.RDD
# 4.0
type(data)

# 4.1 See data
data.take(5)

# 4.2 How many rows
data.count()

# 4.3 Labels
#     collect. It will get all the elements in the RDD into memory for us to work with them. 
labels = data.map(lambda lp: lp.label) 
lab = labels.collect()

# 4.4 Is data balanced?
#     Apply a map function to every element of list
#      map(func, *iterables) --> map object
labels.countByValue()
noOfOnes = sum(list(map(lambda f: f !=0.0, lab)))     
noOfZeros = sum(list(map(lambda f: f == 0.0, lab)))   

# 4.4.1
ratio = noOfOnes/noOfZeros
ratio


# 5. Split the data into training and test sets (30% held out for testing)
# Ref: https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame.randomSplit
(trainingData, testData) = data.randomSplit([0.7, 0.3])


# 6. Train a RandomForest model.
#    Empty categoricalFeaturesInfo indicates all features are continuous.
#    Note: Use larger numTrees in practice.
#    Setting featureSubsetStrategy="auto" lets the algorithm choose.
model = RandomForest.trainClassifier(trainingData, numClasses=2, categoricalFeaturesInfo={},
                                     numTrees=10, featureSubsetStrategy="auto",
                                     impurity='gini', maxDepth=4, maxBins=32)

# 7. Evaluate model on test instances and compute test error
#  LabeledPoint is a case class, and the constructors is: new LabeledPoint(label: Double, features: Array[Double])
#  label	Label for this data point.
#  features	List of features for this data point. 

# 7.1 Extract predictors from testData
testData.take(3)
out = testData.map(lambda x: x.features)
out.take(3)

# 7.2 Predict now
predictions = model.predict(out)

# 7.3 Extract actual target
#     By using the map transformation in Spark, we can apply a function to every element in our RDD. 
target = testData.map(lambda lp: lp.label)
target.take(3)

# 7.4 Couple (zip) actuals and predicted
#    https://stackoverflow.com/questions/29268210/mind-blown-rdd-zip-method

# 7.4.1
x = [1,2,3]
y= [3,4,5]
list(zip(x,y))

labelsAndPredictions = target.zip(predictions)

# 7.5 
mismatch = labelsAndPredictions.filter(lambda lp: lp[0] != lp[1])
testErr = mismatch.count() / float(testData.count())
testErr

# 8.0
metrics = BinaryClassificationMetrics(labelsAndPredictions)
print("Area under Receiver Operating Characteristic (ROC) curve: %.3f" % (metrics.areaUnderROC * 100))
print("Area under Precision/Recall (PR) curve: %.f" % (metrics.areaUnderPR * 100))
    
# 9. Learned classification forest model
print(model.toDebugString())


###################################
