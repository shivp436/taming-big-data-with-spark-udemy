from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("FriendsCounter").getOrCreate()

people_df = (spark
             .read
             .option("header", "true")
             .option("inferSchema", "true")
             .csv("../data/fakefriends-header.csv")
             .cache()
             )

print(f"\nPrint DF Schema")
people_df.printSchema()
print(f"\nExample Data")
people_df.show(5)

print(f"\nShow Only Name, Age of Top 5 People")
people_df.select("name", "age").show(5)

print(f"\nShow people less than 21Y of Age")
people_df.filter(people_df.age < 21).show(5)

print(f"\nShow Count of People by Age")
people_df.groupBy("age").count().show(5)

print(f"\nMake everyone 10Y Older")
people_df.select("name", (people_df.age+10).alias("New Age")).show(5)

spark.stop()
