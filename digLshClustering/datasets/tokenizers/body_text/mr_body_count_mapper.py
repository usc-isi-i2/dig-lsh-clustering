#!/usr/bin/env python

import sys
import json

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
            if body_json["hasBodyPart"]["text"]:
            	print '%s\t%s' % (key, 1)
        except:
            pass

