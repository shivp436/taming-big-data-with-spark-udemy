from __future__ import print_function
from pyspark.sql import SparkSession, functions as F, types as T 
from pyspark.ml.regression import LinearRegression
from pyspark.ml.linalg import Vectors

def loadInputData(spark):
    colNames = ['label', 'features']
    ipDF = (spark.sparkContext
             .textFile('../data/regression.txt')
             .map(lambda x: x.split(','))
             .map(lambda x: (float(x[0]), Vectors.dense(float(x[1]))))
             .toDF(colNames)
             )
    return ipDF

def createLinearRegressionModel(trainDF):
    lr = LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)
    model = lr.fit(trainDF)
    return model

def main():
    spark = SparkSession.builder.appName('RegressionModel').getOrCreate()
    
    ipDF = loadInputData(spark)
    trainDF, testDF = ipDF.randomSplit([0.5, 0.5])

    model = createLinearRegressionModel(trainDF)

    # Generate Predictions
    predictions = model.transform(testDF).cache()
    predictions.show(5, truncate=False)

    values = predictions.select('prediction').rdd.map(lambda x: x[0])
    labels = predictions.select('label').rdd.map(lambda x: x[0])
    finalPredictions = values.zip(labels).collect()

    for prediction in finalPredictions[:10]:
        print(prediction)

    spark.stop()

if __name__ == '__main__':
    main()

