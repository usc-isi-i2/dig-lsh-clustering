#!/usr/bin/env python

import sys
import hashlib

# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    #input is the LSH file:
    #   lshkey  key minhash
    (lshkey, key, minhash) = line.split("\t", 2)
    cluster_id = hashlib.sha1(minhash).hexdigest()
    print "%s\t%s\t%s" % (lshkey, cluster_id, minhash)

exit(0)

