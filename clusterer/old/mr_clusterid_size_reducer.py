#!/usr/bin/env python

import sys

current_id = None

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    (cluster_id, size) = line.split('\t', 1)

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if current_id == cluster_id:
        continue
    else:
        print '%s\t%s' % (cluster_id, size)
        current_id = cluster_id

exit(0)
