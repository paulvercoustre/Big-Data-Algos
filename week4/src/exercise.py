"""
~~~~ Instructions

Your goal, in this exercise, is to train and evaluate an image classification
model. You are going to create the following workflow:

1. Load the image labels from the training and the testing dataset: labels are
   stored in the 'data/oxford-IIIT-pet-dataset/annotations/' folder.
2. Select a random subset of training and testing images, for cross-validation.
3. Compute and save the features for each image (load in case the features were
   already computed). The code to compute the features of an image is given in the
   'image_features.py' script. Feel free to adapt this code to your particular use
   case. It is suggested to store the features in the dedicated 'features' folder.
4. Train a classification model on the training dataset (e.g: Naive Bayes for
   multi-class or Linear SVM for binary classification)
5. Evaluate the quality of the classification model on the testing dataset.

This script should be run as follows:

    ./spark-2.1.0-bin-hadoop2.7/bin/spark-submit ./src/exercise.py ./data/oxford-IIIT-pet-dataset/

~~~~ Recommendations

Steps 1-2 could be performed by hand, without Spark, since we don't have much
data, but please don't do that.

You are encouraged to prototype your code on a small subset of the full dataset;
you may achieve this by selecting a small number of random samples in step #2,
or by creating a new dataset manually.

If you do not have enough RAM to train the classification model on the full
dataset, you will have to train your model on a Spark cluster. To do so, you
will have to share data using a local, pseudo-cluster HDFS.

To run the feature computation step, you will need to install the following
requirements:

    # You probably want to do this in a virtualenv
    pip install keras tensorflow h5py pillow numpy

Do not try to obtain the best possible classification scores at all cost;
instead, focus on producing correct, optimized Spark code.

Submit on the 31th may to spark@behmo.com

~~~ Documentation

- Spark standard library: https://spark.apache.org/docs/latest/programming-guide.html#transformations
- Classification with Spark: https://spark.apache.org/docs/latest/mllib-classification-regression.html
- Multi-label classification evaluation: https://spark.apache.org/docs/latest/mllib-evaluation-metrics.html#multilabel-classification
"""
import os
import sys
import pyspark
import itertools as iter
import numpy as np
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import RandomForest, RandomForestModel


DATASET_ROOT = os.path.join(os.path.dirname(__file__), "..", "data", "oxford-IIIT-pet-dataset")
ANNOTATIONS_ROOT = os.path.join(DATASET_ROOT, "annotations")
IMAGE_ROOT = os.path.join(DATASET_ROOT, "images")
FEATURES_ROOT = os.path.join(DATASET_ROOT, "features")

seed = 12345

def main():
  sc = pyspark.SparkContext()
  annotations = sc.textFile(ANNOTATIONS_ROOT + "/trainval.txt")
  annotations_test = sc.textFile(ANNOTATIONS_ROOT +  "/test.txt")

  # labels is an RDD - labels have to start at 0 hence "-1"
  labels = annotations.map(lambda s: [s.split()[0], int(s.split()[1]) - 1])

  # split the training set in 80% train / 20% cross-validation
  training_labels, cross_val_labels = labels.randomSplit([0.8,0.2], seed)

  # labels_test is an RDD
  test_labels = annotations_test.map(lambda s: [s.split()[0], int(s.split()[1]) - 1])

  # create the label - features pairs to train a classifier by importing features
  trainData = training_labels.map(lambda s: LabeledPoint(s[1], \
    np.load(FEATURES_ROOT + "/" + s[0] + ".jpg.npy")))

  crossValData = cross_val_labels.map(lambda s: LabeledPoint(s[1], \
    np.load(FEATURES_ROOT + "/" + s[0] + ".jpg.npy")))

  testData = test_labels.map(lambda s: LabeledPoint(s[1], \
    np.load(FEATURES_ROOT + "/" + s[0] + ".jpg.npy")))


  ### grid search function ###
  numTrees = [10, 12, 15]
  maxDepth = [10, 12, 15]

  bestErr = 1
  bestParams = np.zeros(2)

  for i, j in iter.product(range(len(numTrees)), range(len(maxDepth))):

    # train a RandomForest model
    model = RandomForest.trainClassifier(trainData, numClasses=37, categoricalFeaturesInfo={},\
                                            numTrees=numTrees[i], featureSubsetStrategy="auto",\
                                            impurity='gini', maxDepth=maxDepth[j], maxBins=32, seed = seed)

    # evaluate model on crossval instances
    predictions = model.predict(crossValData.map(lambda s: s.features))
    labelsAndPredictions = crossValData.map(lambda s: s.label).zip(predictions)
    crossValErr = labelsAndPredictions.filter(lambda (v, p): v != p).count() / float(crossValData.count())
  
    #print('Learned classification forest model:')
    #print(model.toDebugString())

    if crossValErr < bestErr:
      bestParams = [numTrees[i], maxDepth[j]]
      bestErr = crossValErr
      print("Better cross validation error: {}".format(crossValErr))
      print("New parameters: numTree: {}, maxDepth: {}".format(bestParams[0], bestParams[1]))


  # train model with the best parameters
  bestModel = RandomForest.trainClassifier(trainData, numClasses=37, categoricalFeaturesInfo={},\
                                           numTrees=bestParams[0], featureSubsetStrategy="auto",\
                                            impurity='gini', maxDepth=bestParams[1], maxBins=32, seed = seed)

  # evaluate model on test data
  predictions = bestModel.predict(testData.map(lambda s: s.features))
  labelsAndPredictions = testData.map(lambda s: s.label).zip(predictions)
  testErr = labelsAndPredictions.filter(lambda (v, p): v != p).count() / float(testData.count())

  print("Best parameters: numTree: {}, maxDepth: {}".format(bestParams[0], bestParams[1]))
  print('Test Error = ' + str(testErr))

pass

if __name__ == "__main__":
    main()

    # To profile your application with Spark UI, you want to prevent it from finishing
    # DON'T FORGET to comment this line when submitting your app on a Spark cluster!
    #raw_input("Press ctrl+c to exit")
