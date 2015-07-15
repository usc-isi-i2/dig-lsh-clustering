#!/usr/bin/env python

import sys
from pyspark import SparkContext
import json
from tokenizer.inputParser.JSONParser import JSONParser
import re
from utils.address import standardize_state_name

def clean_data(x_list):
    #print "clean:", x_list

    for x in x_list:
        x = remove_country(x)
        fields = re.split(r"[,/]+", x)
        state = fields[len(fields)-1].strip()
        state = standardize_state_name(state)
        cities = fields[0:len(fields)-1]
        for city in cities:
            city = city.strip()
            yield [city, state]

def remove_country(x):
    regExs = [
        re.compile(re.escape('United States'), re.IGNORECASE),
        re.compile(re.escape('United States of America'), re.IGNORECASE),
        re.compile(re.escape('USA'), re.IGNORECASE),
        re.compile(',\\s+$')
    ]
    for regEx in regExs:
        x = regEx.sub('', x)
    #print "clean country:", x
    return x

def generate_csv(key, values):
    final = ""
    sep = ""
    for value in values:
        final = final + sep + str(value)
        sep = "\t"
    return str(key) + "\t" + final

if __name__ == "__main__":
    sc = SparkContext(appName="LSH")
    inputFilename = sys.argv[1]
    config_file = open(sys.argv[2])
    config = json.load(config_file)
    outputFilename = sys.argv[3]

    rdd = sc.sequenceFile(inputFilename)
    parser = JSONParser(config, None)
    json_rdd = rdd.mapValues(lambda x: parser.parse_values(x))
    cleaned = json_rdd.flatMapValues(lambda x: list(clean_data(x)))
    result = cleaned.map(lambda (x, y) : generate_csv(x, y))
    result.saveAsTextFile(outputFilename)