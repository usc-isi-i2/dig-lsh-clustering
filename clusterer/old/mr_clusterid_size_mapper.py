#!/usr/bin/env python

import sys
import hashlib

# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    if len(line) > 0:
        (lsh, cluster_id, cluster_count, minhash) = line.split("\t", 3)
        print "%s\t%s" % (cluster_id, cluster_count)
exit(0)

