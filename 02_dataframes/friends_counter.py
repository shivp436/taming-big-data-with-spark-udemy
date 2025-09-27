from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import avg, round

# Create a spark session
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

def mapper(line):
    fields = line.split(',')
    return Row(
        uid=int(fields[0]),
        name=fields[1],
        age=int(fields[2]),
        friends=int(fields[3])
    )

file = spark.sparkContext.textFile('../data/fakefriends.csv')
people = file.map(mapper)

# convert RDD to DataFrame
people_df = spark.createDataFrame(people).cache()
people_df.createOrReplaceTempView("people")

# Filter for People in 13-19 age
teens_df = spark.sql("SELECT * FROM people WHERE age>=13 and age <= 19")

# convert dataframe to list
"""
print(f"\n", "="*50)
print("All Teenagers")
teens_df.show()

for line in teens_df.collect():
    print(line)

print("="*50, f"\n")
"""

# Get average age 
avg_df = (
        teens_df
        .groupBy("age")
        .agg(round(avg("friends"),2).alias("Avg Friends Count"))
        .orderBy("age")
        .withColumnRenamed("age", "Age"))
avg_df.show()

spark.stop()
