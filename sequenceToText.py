#!/usr/bin/env python

from pyspark import SparkContext
import sys



if __name__ == "__main__":
    sc = SparkContext(appName="SequenceToText")
    in_file = sc.sequenceFile( sys.argv[1])
    if len(sys.argv) >= 3 and sys.argv[3] == "--values":
        values = in_file.map(lambda (x, y): y)
    else:
        values = in_file
    values.saveAsTextFile(sys.argv[2])
