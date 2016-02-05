#!/usr/bin/env python

import sys

global_count = 0

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    (key, count) = line.split('\t', 1)
    global_count += int(count)    
print '%s'% str(global_count)

