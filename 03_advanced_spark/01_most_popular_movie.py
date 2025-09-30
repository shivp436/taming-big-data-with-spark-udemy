from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType
import codecs

spark = (SparkSession.builder
         .appName('MostPopularMovie')
         .getOrCreate()
         )

# Load the Ratings DF
ratingSchema = (StructType([
            StructField('userID', IntegerType(), True),
            StructField('movieID', IntegerType(), True),
            StructField('rating', IntegerType(), True),
            StructField('timestamp', LongType(), True)
            ])
          )

ratings_df = (spark.read
             .option('sep', '\t')
             .schema(ratingSchema)
             .csv('../data/ml-100k/u.data')
             .groupBy('movieID')
             .agg(
                 F.countDistinct('userID').alias('watchCount')
                 )
             .orderBy('watchCount', ascending=False)
             .limit(1).cache()
             )

# Load the Movies Info DF
movieSchema = (StructType([
                StructField('movieID', IntegerType(), True),
                StructField('movieName', StringType(), True),
                # Spark will ignore the other columns, as no schema provided
                ])
               )

movies_df = (spark.read
             .option('sep', '|')
             .schema(movieSchema)
             .csv('../data/ml-100k/u.item')
             )

# Join to get Movie Name into Ratings DF
final_df = (ratings_df.alias('r')
            .join(
                movies_df.alias('m'),
                F.col('r.movieID') == F.col('m.movieID'),
                'left'
                )
            .select('m.movieName', 'r.watchCount')
            )

print(f'\n++++++ Most Popular Movie - Using Join +++++++')
final_df.show()

print(f"""\n
      Using BroadCast to create a movieDict
      - We can create a dict instead of a DF when data is relatively small
      - then send the data out to each executor when needed
      - this dictionary will be available within each cluster
      """)

# Load up the Movie Names as a Dictionary
def loadMovieNames():
    movieNames = {}

    with codecs.open(
            filename = '../data/ml-100k/u.item',
            mode = 'r',
            encoding = 'ISO-8859-1',
            errors = 'ignore'
            ) as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

# Broadcast: this will make the dictionary available across all clusters
nameDict = spark.sparkContext.broadcast(loadMovieNames())

# Look up the MovieName from the Dictionary Object
@F.udf(StringType())
def lookUpMovieName(movieID):
    return nameDict.value.get(movieID)

# Add MovieTitle using UDF
ratings_df = (ratings_df
                .withColumn(
                    'movieName'
                    , lookUpMovieName(F.col('movieID'))
                    )
                )

ratings_df.show()

spark.stop()
