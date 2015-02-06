#!/usr/bin/env python

import sys
import json

STOP_WORDS = ["a", "an", "and", "are", "as", "at", "be", "but", "by",
"for", "if", "in", "into", "is", "it",
"no", "not", "of", "on", "or", "such",
"that", "the", "their", "then", "there", "these",
"they", "this", "to", "was", "will", "with", "-", ";", ",", "_", "+", "/", "\\"]


def tokenize_input(input):
    tokens = unicode(input).split()
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

# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    idx = line.find("\t")
    if idx != -1:
        key = line[0:idx]
        value =  line[idx+1:]
        try:
            body_json = json.loads(value, encoding='utf-8')
            bodyText = body_json["hasBodyPart"]["text"]
            tokens = tokenize_input(bodyText)
            tokensStr = write_tokens(tokens, "\t")
            print '%s\t%s' % (key, tokensStr)
        except:
            pass

