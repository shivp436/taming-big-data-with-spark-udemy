from __future__ import print_function

from pyspark.sql import SparkSession, functions as F, types as T 
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.feature import VectorAssembler

def loadInputData(spark):
    ipDF = (spark.read
            .option('header', 'true')
            .option('inferSchema', 'true')
            .csv('../data/realestate.csv')
            )
    return ipDF

def prepareData(ipDF, inputColumns, outputColumn):
    '''Assembler takes the inputColumns and vectorizes into 1 single outputCol'''
    assembler = (VectorAssembler()
                 .setInputCols(inputColumns)
                 .setOutputCol('features')
                 )

    '''selecting outputColumn, vector(inputColumns) as features'''
    df = assembler.transform(ipDF).select(outputColumn, 'features', *inputColumns)

    trainDF, testDF = df.randomSplit([0.5, 0.5])
    return (trainDF, testDF)

def buildModel(trainDF, outputColumn):
    dtr = DecisionTreeRegressor().setFeaturesCol('features').setLabelCol(outputColumn)
    model = dtr.fit(trainDF)
    return model


def main():
    spark = SparkSession.builder.appName('RealEstateDecisionTree').getOrCreate()

    # load data 
    ipDF = loadInputData(spark)

    # Prepare data for training
    inputColumns = ['HouseAge', 'DistanceToMRT', 'NumberConvenienceStores']
    outputColumn = 'PriceOfUnitArea'
    trainDF, testDF = prepareData(ipDF, inputColumns, outputColumn)

    # Build and train model
    model = buildModel(trainDF, outputColumn)

    # Predict Values
    fullPredictions = model.transform(testDF).cache()
    opDF = (fullPredictions
            .select(
                F.col('PriceOfUnitArea'),
                F.col('prediction').alias('Predicted_PriceOfUnitArea'),
                F.abs(F.col('PriceOfUnitArea')-F.col('Predicted_PriceOfUnitArea')).alias('Error'),
                *inputColumns
            )
            .orderBy('Error')
            .cache()
        )
    opDF.show(5, truncate=False)

    spark.stop()

if __name__ == '__main__':
    main()
