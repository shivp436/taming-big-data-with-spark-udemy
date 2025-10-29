from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("AverageFriendsCounter")
sc = SparkContext(conf=conf)

file = sc.textFile('friends_count.txt')

# remove first line 
friends = file.zipWithIndex().filter(lambda x: x[1] > 0).map(lambda x: x[0])

# Take first 10 lines
# friends = friends.zipWithIndex().filter(lambda x: x[1] < 10).map(lambda x: x[0])

def extract_friend_count(line):
    values = line.strip("[]").split(", ")
    age = int(values[2])
    friends = int(values[3])
    return [age, friends]

rdd = friends.map(extract_friend_count)

# Map (age, friend_count) to (age, (friend_count, 1)) for each entry
rdd = rdd.mapValues(lambda x: (x, 1))  # (age, (friend_count, 1))

# Reduce by key (age) and aggregate friend_count and the count
rdd = rdd.reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1])) # (age, (total_friend_count, total_count))

# get average friend count
rdd = rdd.mapValues(lambda x: round(x[0]/x[1], 2))
sorted_rdd = rdd.sortByKey()
result = sorted_rdd.collect()

# print(rdd.collect())
for r in result:
    age, avg_cnt = r
    print(f"Age: {age} => Average Friends Count: {avg_cnt}")

