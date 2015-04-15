#!/usr/bin/env python

import sys
import hashlib

# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    if len(line) > 0:
        print line

exit(0)

