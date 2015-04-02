#!/usr/bin/env python

import sys

current_key = None

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    (key, score) = line.split('\t', 1)

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if current_key == key:
        continue
    else:
        (key1, key2) = key.split("$$$$", 1)
        print '%s\t%s\t%s' % (key1, key2, score)
        current_key = key

exit(0)
