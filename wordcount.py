from operator import add
from pyspark import SparkContext

sc = SparkContext("local[*]","wordjoy")
lines = sc.textFile("joy.txt")

wc = lines.flatMap(lambda x: x.split(' ')
		).map(lambda x: (x, 1)
		).reduceByKey(add)

for (word, count) in wc.collect():
    print("%s: %i" % (word, count))
