from pyspark.sql import SparkSession, functions as F
import time

spark = SparkSession.builder.appName("WordCounter").getOrCreate()

start = time.time()
words_df = (spark
            .read.text("../data/book.txt")
            .select(
                F.explode(
                    F.split(
                        F.lower(F.col("value")), "\\W+"
                    )
                ).alias("Word")
            )
            .filter(F.col("Word") != "")
            .groupBy("Word")
            .agg(F.count("*").alias("Count"))
            .orderBy("Count", ascending=[0])
            )
print("DF Created in ", time.time()-start, " seconds")

words_df.show(5)
print(f"Total {time.time()-start} seconds")

spark.stop()
