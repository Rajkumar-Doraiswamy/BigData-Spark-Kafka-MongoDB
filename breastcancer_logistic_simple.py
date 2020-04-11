# -*- coding: utf-8 -*-

"""
@author: ashok
Created on Wed Jan 25 13:04:45 2017
Data: https://www.kaggle.com/uciml/breast-cancer-wisconsin-data/kernels

a[start:end] # items start through end-1
a[start:]    # items start through the rest of the array
a[:end]      # items from the beginning through end-1
a[:]         # a copy of the whole array


"""

#%%
#
%reset -f  # Magic comands. Delete objects. Specific code for ipython

## 1. Import needed modules
import os
import numpy as np   # For ndarray operations
import pandas as pd  # data reading and processing

# 2. Set working folder
os.chdir("C:\\Users\\ashok\\OneDrive\\Documents\\breast_cancer")
os.getcwd()    # So where are we
os.listdir()   # Which files are here

#%%          
## Read and explore data

# 3. Read datafile
bc=pd.read_csv("data.csv", sep=",", verbose=True)

# 4. Examine file and its contents
bc.head()
bc.tail()
bc.describe()
bc.std()      # Column wise standard deviation of numerical variables
bc.mean(axis=1)  # Row wise mean
bc.std(axis=0)   # Column wise std


# 4.1 Last column appears to be all NaN
bc['Unnamed: 32'].tail(20)

# 4.2 So drop not needed columns
bc.drop('Unnamed: 32', axis=1, inplace=True)  # Label, 'unnamed: 32' is listed on axis=1
bc.drop('id', axis=1,inplace=True)

# 4.3 Data dimensions
bc.shape
# 4.4 Column names
bc.columns.values
bc.columns
# 4.5 Column data types
bc.dtypes  # diagnosis is target variable

#####################################
#%%
## 5. Process Data
# Standardize features by removing the mean and scaling
#  to unit variance. Centering and scaling happen independently
#   on each feature by computing the relevant statistics on the
#    samples in the training set. Mean and standard deviation
#     are then stored to be used on later data using the 
#      transform method.
# 5.1 Import needed modules
from sklearn.preprocessing import StandardScaler as ss
from sklearn.decomposition import PCA

# 5.2 Separate predictor variables and target class
X=bc.loc[:, 'radius_mean' : 'fractal_dimenson_worst']
# Above is same as:
X=bc.iloc[:, 1 : bc.shape[1]]

# 5.3
X.columns

# 5.4
y=bc.loc[:,'diagnosis']
# OR
y=bc.iloc[:, 0]


# 5.5 y has two unique values
y.unique()  # Number of unique values in y

# 6 Center and scale
#  Initialize the centering/scaling object
scaler=ss()           
# 6.1 Use the object to create model
model=scaler.fit(X)
# 6.2 And now transform data
data_trans=model.transform(X)

# 6.3 Check
data_trans.shape
data_trans.mean()
type(data_trans)

#### 7. PCA now
pca=PCA()  # PCA object first. Instantiate the class

# 7.1 Get PCA model now       
pca_model=pca.fit(data_trans)

# 7.2 How much of variance is explained column-by-column
pca_model.explained_variance_ratio_

# 7.3 Explained variance for first six columns
pca_model.explained_variance_ratio_[:6]

# 7.4 The first 10 components explain 95% of variance
sum(pca_model.explained_variance_ratio_[:10])

# 7.5 The transformed data
final=pca_model.transform(data_trans)
final.shape

###################### Modeling ######################33
#%%

## 8. Import necessary modules
from sklearn.cross_validation import train_test_split as tts
from sklearn.linear_model import LogisticRegression as lr

# 9. Split data in train test, 
#     Only first 10 columns of PCs be taken
train,test,train_y,test_y=tts(final[: ,:10],y, test_size=0.20)
train.shape
test.shape
type(test_y)   # It is pandas Series

# 10. Instantiate Logistic regression model creating object
log_reg=lr()
# Fit the logistic regression object
# 10.1 Get model
model = log_reg.fit(train,train_y)
model

# 11. Make predictions and covert them to pandas Series
result=pd.Series(model.predict(test), index=range(len(test_y))) # Convcert result also

# 11.1 Create a  Data frame of 'actual' and 'predicted' data
# First try this
f_result=pd.DataFrame({'actual' : test_y, 'predicted' : result})
f_result

# 11.2 Index of test_y needs to be reset by dropping existing index
f_result=pd.DataFrame({'actual' : test_y.reset_index(drop=True), 'predicted' : result})
f_result

# 12. So what is the accuracy
sum(f_result['actual'] == f_result['predicted'])/f_result.shape[0]

###############################



