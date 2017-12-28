import sys
import re
from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext(appName="WordCount")
    print '\n '.join(sc.textFile(sys.argv[1])\
	.flatMap(lambda line: re.split('\\s+', line))\
	.map(lambda line: re.sub('[^A-Za-z]+','', line))\
        .map(lambda line: (line.lower(), 1))\
	.reduceByKey(lambda x, y: x + y)\
        .map(lambda (a, b): (b, a))\
        .sortByKey(ascending=False)\
        .map(lambda (a, b): "%s: %s" % (b, a) )\
	.collect()[ :10])
    sc.stop()

