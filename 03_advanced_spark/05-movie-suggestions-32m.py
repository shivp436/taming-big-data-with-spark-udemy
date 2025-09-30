from pyspark.sql import SparkSession, functions as F, types as T 
import sys

def getMovieNames(spark):
    print('Fetching Movie Names')

    movieNames = (spark.read
                  .option('header', 'true')
                  .option('inferSchema', 'true')
                  .csv('../data/ml-32m/movies.csv')
                  .select(
                    F.col('movieId').alias('movieID'),
                    F.col('title').alias('movieName')
                  )
                  )
    return movieNames


def getMovieRatings(spark):
    print('Fetching Movie Ratings')

    movieRatings = (spark.read
                    .option('header', 'true')
                    .option('inferSchema', 'true')
                    .csv('../data/ml-32m/ratings.csv')
                    .select(
                        F.col('userId').alias('userID'),
                        F.col('movieId').alias('movieID'),
                        F.col('rating').cast('int').alias('rating')
                    )
                    )

    return movieRatings

def getMovieRatingPairs(movieRatings, movieID):
    print('Matching Movie Pairs')
    movieRatingPairs = (movieRatings.alias('r1')
                        .join(
                            movieRatings.alias('r2'), 
                            (F.col('r1.userID') == F.col('r2.userID')) & (F.col('r1.movieID') < F.col('r2.movieID'))
                            )
                        .filter(
                            (F.col('r1.movieID') == movieID) | (F.col('r2.movieID')==movieID)
                        )
                        .select(
                            F.col('r1.movieID').alias('movie1'),
                            F.col('r2.movieID').alias('movie2'),
                            F.col('r1.rating').alias('rating1'),
                            F.col('r2.rating').alias('rating2')
                            )
                        )
    return movieRatingPairs

def computeCosineSimilarity(movieRatingPairs):
    print('Computing Similarity Scores')
    pairScores = (movieRatingPairs
                  .withColumns({
                    'xx': F.col('rating1') * F.col('rating1'),
                    'yy': F.col('rating2') * F.col('rating2'),
                    'xy': F.col('rating1') * F.col('rating2')
                  })
                  .groupBy('movie1', 'movie2')
                  .agg(
                    F.sum(F.col('xy')).alias('nume'),
                    (F.sqrt(F.sum(F.col('xx'))) * F.sqrt(F.sum(F.col('yy')))).alias('deno'),
                    F.count(F.col('xy')).alias('numPairs')
                    )
                  .withColumn('score', F.when(F.col('deno')!=0, F.col('nume')/F.col('deno')).otherwise(0))
                  .select('movie1', 'movie2', 'score', 'numPairs')
                  )
    return pairScores

def computePearsonCorrelation(movieRatingPairs):
    print('Computing Pearson Correlation')
    pairScores = (movieRatingPairs
                  .groupBy('movie1', 'movie2')
                  .agg(
                    F.count('rating1').alias('numPairs'),
                    F.avg('rating1').alias('avg1'),
                    F.avg('rating2').alias('avg2'),
                    F.sum(F.col('rating1') * F.col('rating2')).alias('sum_xy'),
                    F.sum(F.col('rating1') * F.col('rating1')).alias('sum_xx'),
                    F.sum(F.col('rating2') * F.col('rating2')).alias('sum_yy')
                  )
                  .withColumn('numerator', 
                    F.col('sum_xy') - (F.col('numPairs') * F.col('avg1') * F.col('avg2')))
                  .withColumn('denom1',
                    F.col('sum_xx') - (F.col('numPairs') * F.col('avg1') * F.col('avg1')))
                  .withColumn('denom2',
                    F.col('sum_yy') - (F.col('numPairs') * F.col('avg2') * F.col('avg2')))
                  .withColumn('score',
                    F.when((F.col('denom1') > 0) & (F.col('denom2') > 0),
                      F.col('numerator') / F.sqrt(F.col('denom1') * F.col('denom2'))
                    ).otherwise(0))
                  .select('movie1', 'movie2', 'score', 'numPairs')
                  )
    return pairScores

def computeJaccardSimilarity(movieRatingPairs, ratingThreshold=3):
    print('Computing Jaccard Similarity')
    # Create binary liked/disliked
    jaccardScores = (movieRatingPairs
                     .select(
                        F.col('movie1'), F.col('movie2'),
                        F.when(F.col('rating1') >= ratingThreshold, 1).otherwise(0).alias('liked1'),
                        F.when(F.col('rating2') >= ratingThreshold, 1).otherwise(0).alias('liked2')
                     )
                     .groupBy('movie1', 'movie2')
                     .agg(
                         F.sum(F.when((F.col('liked1') == 1) & (F.col('liked2') == 1), 1).otherwise(0)).alias('both_liked'),
                         F.sum(F.when((F.col('liked1') == 1) | (F.col('liked2') == 1), 1).otherwise(0)).alias('either_liked'),
                         F.count('*').alias('numPairs')
                     )
                     .select(
                        'movie1', 'movie2',
                        F.when(F.col('either_liked')>0, F.col('both_liked')/F.col('either_liked')).otherwise(0).alias('score'),
                        'numPairs'
                     )
                     )
    return jaccardScores

def combineScores(cosineScores, pearsonScores, jaccardScores):
    print('Combining all Scores')
    combinedScores = (cosineScores.alias('c')
                      .join(pearsonScores.alias('p'), ['movie1', 'movie2'], 'inner')
                      .join(jaccardScores.alias('j'), ['movie1', 'movie2'], 'inner')
                      .select(
                        'movie1', 'movie2',
                        F.col('c.score').alias('cosineScore'),
                        F.col('p.score').alias('pearsonScore'),
                        F.col('j.score').alias('jaccardScore'),
                        F.col('c.numPairs').alias('numPairs')
                      )
                      )
    return combinedScores

def getSimilarMovies(movieID, combinedScores, movieNames):
    cosineScoreThreshold = 0.95
    pearsonScoreThreshold = 0.50
    jaccardScoreThreshold = 0.95
    coOccurenceThreshold = 50.0 

    similarMovies = (combinedScores
                     .filter(
                        ((F.col('movie1')==movieID) | (F.col('movie2')==movieID))
                        & (F.col('cosineScore') > cosineScoreThreshold)
                        & (F.col('pearsonScore') > pearsonScoreThreshold)
                        & (F.col('jaccardScore') > jaccardScoreThreshold)
                        & (F.col('numPairs') > coOccurenceThreshold)
                     )
                     .withColumn('score', (F.col('cosineScore')+F.col('pearsonScore')+F.col('jaccardScore'))/3.0)
                     #.limit(5)
                     .select(
                        F.when(F.col('movie1')==movieID, F.col('movie2')).otherwise(F.col('movie1')).alias('movieID')
                        , F.col('score'), F.col('numPairs')
                        , 'cosineScore', 'pearsonScore', 'jaccardScore'
                     )
                     .join(movieNames, 'movieID', 'left')
                     .select(
                        'movieID',
                        F.trim(F.col('movieName')).alias('Movie Name'),
                        F.concat(F.round(F.col('score')*100, 2), F.lit('%')).alias('Similarity Score'),
                        F.col('numPairs').alias('Strength'),
                        F.round(F.col('cosineScore'), 2).alias('Cosine Score'),
                        F.round(F.col('pearsonScore'), 2).alias('Pearson Score'),
                        F.round(F.col('jaccardScore'), 2).alias('Jaccard Score')
                     )
                     .orderBy(F.desc('Similarity Score'))
                     )
    originalMovieName = movieNames.filter(F.col('movieID')==movieID).select(F.col('movieName')).collect()[0][0]
    return (originalMovieName, similarMovies)

def main():
    if (len(sys.argv) != 2):
        exit("Usage: 04-movie-suggestion.py movieID")
    movieID = int(sys.argv[1])

    spark = SparkSession.builder.appName('MovieRecommender').master('local[*]').getOrCreate()

    movieNames = getMovieNames(spark).cache()
    movieRatings = getMovieRatings(spark).cache()
    movieRatingPairs = getMovieRatingPairs(movieRatings, movieID).cache()

    # Scores
    cosineScores = computeCosineSimilarity(movieRatingPairs).cache()
    pearsonScores = computePearsonCorrelation(movieRatingPairs).cache()
    jaccardScores = computeJaccardSimilarity(movieRatingPairs).cache()

    # Merge all Scores
    combinedScores = combineScores(cosineScores, pearsonScores, jaccardScores).cache()

    # Get Similar Movies
    originalMovieName, similarMovies = getSimilarMovies(movieID, combinedScores, movieNames)

    print(f'\n=== === === Movies Similar to {originalMovieName} === === ===')
    similarMovies.show(truncate=False)

    spark.stop()


if __name__ == '__main__':
    main()
