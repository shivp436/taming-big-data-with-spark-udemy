from pyspark.sql import SparkSession, functions as F, types as T 
from pyspark.ml.recommendation import ALS 
import sys 


def loadMovieNames(spark):
    movieNameSchema = (T.StructType([
        T.StructField('movieID', T.IntegerType(), True),
        T.StructField('movieName', T.StringType(), True)
    ]))

    movieNames = (spark.read
                  .option('sep', '|')
                  .schema(movieNameSchema)
                  .csv('../data/ml-100k/u.item')
                  )

    return movieNames

def loadMovieRatings(spark):
    movieRatingsSchema = (T.StructType([
        T.StructField('userID', T.IntegerType(), True)
        , T.StructField('movieID', T.IntegerType(), True)
        , T.StructField('rating', T.IntegerType(), True)
        # , T.StructField('timestamp', T.LongType(), True)
    ]))

    movieRatings = (spark.read
                    .option('sep', '\t')
                    .schema(movieRatingsSchema)
                    .csv('../data/ml-100k/u.data')
                    )

    return movieRatings

def createUserDF(spark, userID):
    userSchema = T.StructType([
        T.StructField('userID', T.IntegerType(), False)
    ])
    user = spark.createDataFrame([[userID,]], schema=userSchema)
    return user

def getMovieRecommendations(movieRatings, movieNames, user, numMoviesToSuggest):
    als = (ALS()
           .setMaxIter(5)
           .setRegParam(0.01)
           .setUserCol('userID')
           .setItemCol('movieID')
           .setRatingCol('rating')
           )
    model = als.fit(movieRatings)
    recommendations = model.recommendForUserSubset(user, numMoviesToSuggest)

    movieRecommendations = (recommendations
                            .select(
                                F.explode('recommendations').alias('rec')
                            )
                            .select(
                                F.col('rec.movieID').alias('movieID'),
                                F.round(F.col('rec.rating'), 2).alias('predictedRating')
                            )
                            .join(movieNames, 'movieID', 'left')
                            .select('movieName', 'movieID', 'predictedRating')
                            .orderBy(F.desc('predictedRating'))
                            )
    return movieRecommendations

def main():
    '''Suggest Movies Based on UserID'''

    if(len(sys.argv) != 3):
        exit("Usage: 01-movie-suggestions-als.py <UserID> <Number of Movies to Suggest>")
    userID = int(sys.argv[1])
    numMoviesToSuggest = int(sys.argv[2])

    spark = SparkSession.builder.appName('ALSMovieRecommender').getOrCreate()

    # Load Data 
    movieNames = loadMovieNames(spark)
    movieRatings = loadMovieRatings(spark)

    # Create user DF (with only 1 userID)
    user = createUserDF(spark, userID)

    # Show recommendations
    movieRecommendations = getMovieRecommendations(movieRatings, movieNames, user, numMoviesToSuggest)
    print(f'\n === === Top {numMoviesToSuggest} movie recommendations for user {userID} === === ')
    movieRecommendations.show(truncate=False)


if __name__ == '__main__':
    main()
