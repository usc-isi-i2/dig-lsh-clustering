#!/usr/bin/env python

import sys
find_cluster_id = '000a859879e68819b77570b241fa9c63b7b005e9'

# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    if len(line) > 0:
        parts = line.split("\t")
        key = parts[0]
        cluster_id = parts[1]
        if cluster_id == find_cluster_id:
            print line