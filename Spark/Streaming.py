from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext


def hot(x):
    y = x.filter(lambda x: x[1] > 3)
    print(y.collect())


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: hdfs_wordcounts.py <directory>", file=sys.stderr)
        sys.exit(-1)

    sc = SparkContext(appName="PythonStreamingWordCount")
    ssc = StreamingContext(sc, 15)

    lines = ssc.textFileStream(sys.argv[1])
    counts = lines.flatMap(lambda line: line.split("\n")) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(lambda a, b: a + b)

    counts.pprint()

    count = counts.map(lambda x: ("all_times:", x[1])).reduceByKey(lambda x, y: x + y)
    count.pprint()

    counts.foreachRDD(lambda x: hot(x))

    ssc.start()
    ssc.awaitTermination()