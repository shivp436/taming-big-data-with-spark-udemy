from pyspark import SparkConf, SparkContext
import collections

# Set up the SparkContext
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
# sparkConf
# .setMaster(local) => sets the local machine as master node
# .setAppName("RatingsCounter") => gives the app a name

sc = SparkContext(conf=conf)
# a convention to use sc = sparkContext(conf= conf)

# Load the file and Rating Column
lines = sc.textFile("./ml-100k/u.data")
# textFile reads each line as one value
# so to get the ratings column, we will need to split it out of each line

ratings = lines.map(lambda x: x.split()[2])  # Extract the rating (third column)

# Count ratings
result = ratings.countByValue()

# Unordered Counts by Value
for key, value in result.items():
    print(f"Rating:{key} => Count:{value}")

# Sort the results
sortedResults = collections.OrderedDict(sorted(result.items()))

# Print sorted results
for key, value in sortedResults.items():
    print("%s %i" % (key, value))

