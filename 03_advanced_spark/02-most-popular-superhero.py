from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import codecs

spark = (SparkSession.builder
         .appName('MarvelSuperHero')
         .getOrCreate()
         )

"""
# Broadcast HeroNameDict
def getHeroNameDict():
    heroNameDict = {}
    with codecs.open(
            filename = '../data/Marvel+Names',
            mode = 'r',
            encoding = 'ISO-8859-1',
            errors = 'ignore'
            ) as f:
        for line in f:
            fields = line.strip().split(' ', 1)
            if len(fields) == 2:
                hID, hName = fields
                heroNameDict[int(hID)] = hName.strip()
    return heroNameDict

heroNameDict = spark.sparkContext.broadcast(getHeroNameDict())

# Create a Lookup UDF to get heroName
@F.udf(StringType())
def lookupHeroName(heroID):
    return heroNameDict.value.get(heroID, 'Unknown Hero')
"""

# Load Hero Names as DataFrame
heroNamesSchema = (StructType([
                    StructField('heroID', IntegerType(), True),
                    StructField('heroName', StringType(), True)
                    ])
                   )

names_df = (spark.read
            .option('sep', ' ')
            .option('quote', '\"')
            .schema(heroNamesSchema)
            .csv('../data/Marvel+Names')
            )

# Load & Process the Connections DF
connection_df = (spark.read.text('../data/Marvel+Graph')
                 .withColumn('tokens', F.split(F.col('value'), r'\s+'))
                 .withColumn('heroID', F.col('tokens')[0].cast('int'))
                 .withColumn(
                     'connections', 
                     F.slice(F.col('tokens'), 2, F.size(F.col('tokens')) - 1)
                     )
                 .select('heroID', F.explode('connections').alias('connection'))
                 .dropDuplicates()
                 .groupBy('heroID')
                 .agg(F.count('connection').alias('totalConnectionCount'))
                 .join(names_df, on='heroID', how='left')  
                 .select('heroName', 'heroID', 'totalConnectionCount')  
                 .orderBy(F.desc('totalConnectionCount'))
                 .cache()
                 )

connection_df.show()

spark.stop()
