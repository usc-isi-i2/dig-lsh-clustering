#!/usr/bin/env python

import sys
import re
from utils.address import standardize_state_name
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def clean_data(x_list):
    #print "clean:", x_list

    for x in x_list:
        x = remove_country(x)
        fields = re.split(r"[,/-]+", x)
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
        final = final + sep + str(value.encode('utf-8'))
        sep = "\t"
    if len(final) > 0:
        return str(key) + "\t" + final

def parse(parser, x):
    return parser.parse_values(x)

if __name__ == "__main__":
    from pyspark import SparkContext
    from tokenizer.inputParser.JSONParser import JSONParser
    import json

    sc = SparkContext(appName="LSH")
    inputFilename = sys.argv[1]
    config_file = open(sys.argv[2])
    config = json.load(config_file)
    # config = { "fieldConfig": { "0": { "path": "availableAtOrFrom.address.name" } } }
    outputFilename = sys.argv[3]

    rdd = sc.sequenceFile(inputFilename)
    parser = JSONParser(config, None)
    json_rdd = rdd.mapValues(lambda x: parse(parser, x))

    cleaned = json_rdd.flatMapValues(lambda x: list(clean_data(x)))
    result = cleaned.map(lambda (x, y) : generate_csv(x, y))
    result.filter(lambda line: line is not None).saveAsTextFile(outputFilename)

# if __name__ == "__main__":
#     inputFilename = sys.argv[1]
#     outputFilename = sys.argv[2]
#
#     in_file = open(inputFilename)
#     out_file = open(outputFilename, 'w')
#
#     for line in in_file:
#         line_parts = line.split("\t")
#         uri = line_parts[0]
#         value = line_parts[1]
#         clean_values = list(clean_data([value]))
#         for clean in clean_values:
#             out_file.write(generate_csv(uri, clean) + "\n")
#
#     out_file.close()
