# Last amended: 06/08/2019
#  Ref:
# 1.https://github.com/databricks/spark-deep-learning#working-with-images-in-spark
# 2. https://spark-packages.org/package/databricks/spark-deep-learning
#
#		Objectives:
#					Learn to use deep learning in spark
#					Transfer Learning in spark


"""
Initial Steps
	1. Start hadoop:

                    ./allstart.sh

	2. Unzip cats_dogs.tar.gz file in folder: /home/ashok/.keras/datasets
	3. Transfer all image files to hadoop.  File transfer takes lot of time

		cd ~
   		hdfs dfs -put /home/ashok/.keras/datasets/cats_dogs  hdfs://localhost:9000/user/ashok/data_files

	4. Start pyspark. We allocate minimum 3gb to spark, to start with.
		If you have more RAM, allocate more RAM.

		cd ~
		pyspark  --packages databricks:spark-deep-learning:1.5.0-spark2.4-s_2.11      --driver-memory 3g

"""

## Spark program. Use %paste to copy and paste

# 1.0 Call libraries
# 1.1 Library to import images from hdfs into spark

from pyspark.ml.image import ImageSchema

# 1.2 Library to create a column of  literal value.
#        https://spark.apache.org/docs/2.2.0/api/python/pyspark.sql.html#pyspark.sql.functions.lit

from pyspark.sql.functions import lit

# 1.3 Spark MLlib 
#        https://spark.apache.org/docs/latest/ml-classification-regression.html#binomial-logistic-regression

from pyspark.ml.classification import LogisticRegression

# 1.4
# https://spark.apache.org/docs/2.2.0/api/java/org/apache/spark/ml/evaluation/MulticlassClassificationEvaluator.html

from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# 1.5 ML Pipelines provide a uniform set of high-level APIs built on top of DataFrames 
#         that help users create and tune practical machine learning pipelines.
# Ref: https://spark.apache.org/docs/latest/ml-pipeline.html

from pyspark.ml import Pipeline

# 1.6
#        https://github.com/databricks/spark-deep-learning
#        https://www.kdnuggets.com/2018/05/deep-learning-apache-spark-part-2.html
#       The DeepImageFeaturizer automatically peels off the last layer of a pre-trained 
#        neural network and uses the output from all the previous layers as features for 
#        the logistic regression algorithm


from sparkdl import DeepImageFeaturizer

# 1.7 Misc
import time

# 2.0 Read images from hadoop

image_df_dogs = ImageSchema.readImages("hdfs://localhost:9000/user/ashok/data_files/cats_dogs/train/dogs")
image_df_cats = ImageSchema.readImages("hdfs://localhost:9000/user/ashok/data_files/cats_dogs/train/cats")

# 2.1 Show what is read:

image_df_dogs.show(5)
image_df_cats.show(5)

# 2.2 Datatype

type(image_df_dogs)

# 2.3 Add a label column with assigned value. Our data is binary.

image_df_cats = image_df_cats.withColumn("label", lit(0))            # Column holds value 0
image_df_dogs = image_df_dogs.withColumn("label", lit(1))        # Column holds value 1

# 2.4
#image_df_dogs.head(2)   # AVOID THIS
#image_df_cats.head(2)     # AVOID THIS

# 2.5 Get column names. These are same for both dataframes

image_df_dogs.columns       # ['image', 'label']
image_df_cats.columns

# 3.0 Merge two dataframes, row-wise

df = image_df_cats.union(image_df_dogs)
df.count()             # 2000

# 4.0 Extract features of data using ResNet50
#         You can also use your own simple model (after giving it a name & saving)

featurizer = DeepImageFeaturizer(inputCol="image", 
                                                                        outputCol="features",
                                                                        modelName="ResNet50"
                                                                        )

# 4.1 Perform logistic regression on extracted features:

lr = LogisticRegression(maxIter=20,
                                                 regParam=0.05,
                                                elasticNetParam=0.3,
                                                labelCol="label"
                                                )

# 4.2 Add to pipeline operations of feature-extraction and logistic regression

p = Pipeline(stages=[featurizer, lr])

# 5.0 Start learning model:

start = time.time()
model = p.fit(df)    
end = time.time()
print  ((end-start)/60)         # 10 minutes


# 6.0 Inspect training error on a part of training images

df_new = model.transform(df.limit(10)).select("image", "probability", "label", "prediction")

# 6.1

predictionAndLabels = df_new.select("prediction", "label")

# 6.2

evaluator = MulticlassClassificationEvaluator(metricName="accuracy")

# 6.3 Accuracy?

print("Training set accuracy = " + str(evaluator.evaluate(predictionAndLabels)))

# 7.0 Validation data accuracy:
# 7.1 Create Validation data Dataframe and add 'label' column

val_df_dogs = ImageSchema.readImages("hdfs://localhost:9000/user/ashok/data_files/cats_dogs/validation/dogs")
val_df_cats = ImageSchema.readImages("hdfs://localhost:9000/user/ashok/data_files/cats_dogs/validation/cats")
val_df_cats = image_df_cats.withColumn("label", lit(0))
val_df_dogs = image_df_dogs.withColumn("label", lit(1))

# 8.0 Create a dataframe of 20 images each from cats and dogs

df_val = val_df_cats.limit(20).union(val_df_dogs.limit(20))
df_val.count()          # 20

# 8.1 Predict now

df_new_val = model.transform(df_val).select("image", "probability", "label", "prediction")

# 8.2

predictionAndLabels = df_new_val.select("prediction", "label")

# 8.3 Evaluate

evaluator = MulticlassClassificationEvaluator(metricName="accuracy")

# 8.4 Accuracy?

start = time.time()
print("Training set accuracy = " + str(evaluator.evaluate(predictionAndLabels)))
end = time.time()
print((end-start)/60)

################################


