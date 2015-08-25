#!/usr/bin/env python

if __name__ == "__main__":
    from pyspark import SparkContext
    import json
    import sys

    sc = SparkContext(appName="sample")
    inputFilename = sys.argv[1]
    outputFilename = sys.argv[2]

    rdd = sc.sequenceFile(inputFilename).mapValues(lambda x: json.loads(x))
    rdd2 = rdd.map(lambda (x, y): json.dumps(y))
    rdd2.saveAsTextFile(outputFilename)
