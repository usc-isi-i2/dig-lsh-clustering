#!/usr/bin/env python

import sys

global_count = 0

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    global_count += 1
print '%s' % str(global_count)

