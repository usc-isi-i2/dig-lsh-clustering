#!/usr/bin/env python

import sys
import json

for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    if len(line) > 0:
        (key1, key2, score) = line.split('\t', 2)
        jsonObj = json.loads("{\"Column_1\":\"" + key1 + "\", \"Column_2\":\"" + key2 + "\", \"Column_3\":\"" + score + "\"}")
        print "%s\t%s" % (key1, json.dumps(jsonObj))
	#print json.dumps(jsonObj)

exit(0)



