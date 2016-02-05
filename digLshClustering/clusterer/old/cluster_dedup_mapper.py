#!/usr/bin/env python

import sys

# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    (x, y, rest) = line.split("\t", 2)
    key = x + "$$$$" + y
    print key + "\t" + rest

