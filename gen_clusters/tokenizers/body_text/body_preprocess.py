__author__ = 'dipsy'

import json
import sys
import codecs

STOP_WORDS = ["a", "an", "and", "are", "as", "at", "be", "but", "by",
"for", "if", "in", "into", "is", "it",
"no", "not", "of", "on", "or", "such",
"that", "the", "their", "then", "there", "these",
"they", "this", "to", "was", "will", "with", "-", ";", ",", "_", "+", "/", "\\",
" ","*", "i", "am", "my", "me", "have"]


def tokenize_input(input):
    tokens = unicode(input).lower().split()
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
    print "Usage: body_preprocess.py <input json filename> <output filename> [<token separator=\\t>]"
    exit(1)

if len(sys.argv) < 3:
    die()

inputFile = sys.argv[1]
outputFile = sys.argv[2]
separator = "\t"

if len(sys.argv) > 3:
    separator = sys.argv[3]

#out = open(outputFile, "w", encoding="utf-8")
out = codecs.open(outputFile, encoding='utf-8', mode='w')

file = open(inputFile, "r")
data = json.load(file)

hits = data["hits"]["hits"]
for row in hits:
    if row.has_key("_source"):
        source = row["_source"]
        uri = source["uri"]
        if source.has_key("hasBodyPart"):
            bp = source["hasBodyPart"]
            if bp.has_key("text"):
                tc = bp["text"]
                tokens = list(tokenize_input(tc))

                key = uri
                # print("Adding:" + str(tokens))
                out.write(key + "\t" + write_tokens(tokens, separator) + "\n")

file.close()
out.close()
