#!/usr/bin/env python

import sys

current_key = None
current_score = 0
key = None

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    (key, score) = line.split('\t', 1)


    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if current_key == key:
        current_score = score
    else:
        if current_key:
            # write result to STDOUT
            (key1, key2) = current_key.split("$$$$", 1)
            print '%s\t%s\t%s' % (key1, key2, current_score)
        current_count = score
        current_key = key

# do not forget to output the last word if needed!
if current_key == key:
    print '%s\t%s' % (current_key, current_score)