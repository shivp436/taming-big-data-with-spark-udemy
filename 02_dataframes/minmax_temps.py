from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
import time

spark = SparkSession.builder.appName("MinMaxTemp").getOrCreate()

schema = (StructType([
            StructField("stationID", StringType(), True),
            StructField("date", IntegerType(), True),
            StructField("measure", StringType(), True),
            StructField("value", FloatType(), True)
            ])
          )

print("\n=== STRUCT COMPARISON RULES ===")
print("""
When Spark compares structs, it follows LEXICOGRAPHIC ORDER:
1. Compares fields from LEFT TO RIGHT
2. First field has HIGHEST PRIORITY
3. If first fields are equal, compares second field, and so on""")
print("////////////////////////////////\n")

start_time = time.time()
temps_df = (spark.read.schema(schema).csv("../data/1800.csv")
            .filter(F.col("measure").isin('TMIN', 'TMAX'))
            .withColumn("value_date_struct", F.struct("value", "date"))
            .drop("date", "value")
            .cache()
            )

minmax_df = (temps_df
             .groupBy("stationID")
             .agg(
                 # similar to SQL:
                 # Min(Case when Measure='TMIN' THEN value_date_struct END) AS min_struct
                 F.min(
                     F.when(
                         F.col("measure") == 'TMIN'
                         , F.col("value_date_struct")
                         )
                     ).alias("min_struct"),
                 # Max(Case when Measure='TMAX' THEN value_date_struct END) AS max_struct
                 F.max(
                     F.when(
                         F.col('measure') == 'TMAX'
                         , F.col("value_date_struct")
                         )
                     ).alias("max_struct")
                 )
             .select(
                 "stationID"
                 , F.col("min_struct.value").alias("minTemp")
                 , F.to_date(F.col("min_struct.date"), 'yyyyMMdd').alias("minTempDate")
                 , F.col("max_struct.value").alias("maxTemp")
                 , F.to_date(F.col("max_struct.date"), 'yyyyMMdd').alias("maxTempDate")
                 )
             )

minmax_df.show()

print(f"Total: {time.time()-start_time} sec")

spark.stop()
