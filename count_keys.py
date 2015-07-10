#!/usr/bin/env python

from pyspark import SparkContext
import sys



if __name__ == "__main__":
    def extractNumLines(line):
        global lines
        lines += 1
        # print lines, ":", line, "\n\n"
        return line

    sc = SparkContext(appName="CountKeys")
    file = sc.sequenceFile( sys.argv[1])
    rdd = file.reduceByKey(lambda x, y:  x)

    lines = sc.accumulator( 0)
    num_lines = rdd.map(extractNumLines)
    num_lines.collect()
    print "Num lines: %d" % lines.value
