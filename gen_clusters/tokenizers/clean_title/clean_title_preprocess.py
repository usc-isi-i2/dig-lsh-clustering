__author__ = 'dipsy'

import json
import sys

STOP_WORDS = ["a", "an", "and", "are", "as", "at", "be", "but", "by",
"for", "if", "in", "into", "is", "it",
"no", "not", "of", "on", "or", "such",
"that", "the", "their", "then", "there", "these",
"they", "this", "to", "was", "will", "with", "-", ";", ",", "_", "+", "/", "\\"]


def tokenize_input(input):
    tokens = str(input).split()
    for token in tokens:
        if not token in STOP_WORDS:
            yield token


def write_tokens(tokens, sep):
    outStr = ""
    usep = ""
    for token in tokens:
        outStr = outStr + usep + token
        usep = sep
    return outStr

def die():
    print "Please input the required parameters"
    print "Usage: clean_title_preprocess.py <input json filename> <output filename> [<token separator=\\t>]"
    exit(1)

if len(sys.argv) < 3:
    die()

inputFile = sys.argv[1]
outputFile = sys.argv[2]
separator = "\t"

if len(sys.argv) > 3:
    separator = sys.argv[3]

out = open(outputFile, "w")

file = open(inputFile, "r")
data = json.load(file)

hits = data["hits"]["hits"]
for row in hits:
    if row.has_key("_source"):
        source = row["_source"]
        uri = source["uri"]
        if source.has_key("hasFeatureCollection"):
            fc = source["hasFeatureCollection"]
            if fc.has_key("text_title_clean_feature"):
                tc = fc["text_title_clean_feature"]["featureValue"]
                tokens = list(tokenize_input(tc))

                key = uri + ":" + ' '.join(tokens)
                # print("Adding:" + str(tokens))
                out.write(key + "\t" + write_tokens(tokens, separator) + "\n")

file.close()