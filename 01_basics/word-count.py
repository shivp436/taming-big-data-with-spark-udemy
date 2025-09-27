from pyspark import SparkConf, SparkContext
import re

conf = SparkConf().setMaster('local').setAppName('WordCounter')
sc = SparkContext(conf=conf)

inp = sc.textFile('book.txt')

def parse_words(line):
    words = re.compile(r'\W+', re.UNICODE).split(line.lower())
    return [word for word in words if len(word)>2]

words = (inp
         .flatMap(parse_words)
         .map(lambda word: (word, 1))
         .reduceByKey(lambda a, b: a+b)
         .sortBy(lambda x: x[1], ascending=False)
         )

print("\n" + "="*50)

top_words = words.take(10)
for word, count in top_words:
    print(word, count)

print("\n" + "="*50)

sc.stop()
