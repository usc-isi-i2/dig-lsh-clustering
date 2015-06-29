#!/usr/bin/env python

import sys
import json
import traceback
import re

STOP_WORDS = ["a", "an", "and", "are", "as", "at", "be", "but", "by",
              "for", "if", "in", "into", "is", "it",
              "no", "not", "of", "on", "or", "such",
              "that", "the", "their", "then", "there", "these",
              "they", "this", "to", "was", "will", "with", "-", ";", ",", "_", "+", "/", "\\"]

def asciiChars(x):
    "Remove non-ascii chars in x replacing consecutive ones with a single space"
    return re.sub(r'[^\x00-\x7F]+', ' ', x)

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
        value = line[idx+1:]
        try:
            body_json = json.loads(value, encoding='utf-8')
            if body_json.get("hasBodyPart"):
                bodyText = body_json["hasBodyPart"]["text"]
                if type(bodyText) is str or type(bodyText) is unicode:
                    bodyText = bodyText.strip()
                else:
                    bodyText = " ".join(bodyText).strip()
                if len(bodyText) > 0:
                    #bodyText = asciiChars(bodyText)
                    tokens = tokenize_input(bodyText)
                    tokensStr = write_tokens(tokens, "\t")
                    print '%s\t%s' % (key, tokensStr.encode('utf-8'))
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
            sys.stderr.write("Error in Body Mapper:" + str(lines) + "\n")
            sys.stderr.write("Error was caused by data:" + value + "\n")
            pass

exit(0)

