#!/usr/bin/env python

import sys
import hashlib

# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    if len(line) > 0:
        #input is the LSH file:
        #   lshkey  key minhash
        (lshkey, key, minhash) = line.split("\t", 2)
        cluster_id = hashlib.sha1(minhash).hexdigest()
        print "%s\t%s" % (key, cluster_id)

exit(0)

