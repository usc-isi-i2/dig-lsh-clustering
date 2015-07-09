#!/usr/bin/env python

from pyspark import SparkContext
import sys

def extractNumLines(line):
    global lines
    lines += 1

if __name__ == "__main__":
    sc = SparkContext(appName="CountKeys")
    file = sc.sequenceFile( sys.argv[1])
    rdd = file.reduceByKey(lambda x, y:  x)

    lines = sc.accumulator( 0)
    num_lines = sc.flatMap(extractNumLines)
    print "Num lines: %d" % lines.value