from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("FriendsCount").getOrCreate()

people_df = (spark
             .read
             .option("header", "true")
             .option("inferSchema", "true")
             .csv("../data/fakefriends-header.csv")
             .cache()
             )

avg_df = (people_df
          .groupBy("age")
          .agg(F.round(F.avg("friends"),2).alias("Avg Friends Count"))
          .orderBy("age")
          .withColumnRenamed("age", "Age")
          )

avg_df.show(5)

spark.stop()
