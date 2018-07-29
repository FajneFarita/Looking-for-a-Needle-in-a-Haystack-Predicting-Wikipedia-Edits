import pandas as pd
import numpy as np
import sklearn
import time
from sklearn import metrics
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, precision_recall_fscore_support, confusion_matrix, precision_score, recall_score
from sklearn.model_selection import KFold, train_test_split


t0 = time.time()
dataframe = pd.read_csv('aggregate-20160501n.csv')
t1 = time.time()
print(str((t1-t0)/60) + " minutes")

