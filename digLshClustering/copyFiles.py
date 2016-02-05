#!/usr/bin/env python

from pyspark import SparkContext
import sys



if __name__ == "__main__":
    sc = SparkContext(appName="CopyText")
    in_file = sc.textFile( sys.argv[1])
    in_file.saveAsTextFile(sys.argv[2])
