#!/usr/bin/env python

import sys

# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    parts = line.split("\t")
    key = parts[0] + "$$$$" + parts[1]
    value = parts[2]
    print key + "\t" + value

