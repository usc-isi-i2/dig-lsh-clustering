#!/usr/bin/env python

import sys


current_lsh = None
current_clusterid = None
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    (lsh, cluster_id, minhash) = line.split('\t', 2)

    # this IF-switch only works because HADOOP sorts map output
    # by key (here: word) before it is passed to the reducer
    if current_lsh == lsh:
        if current_clusterid == cluster_id:
            continue

    print line
    current_clusterid = cluster_id
    current_lsh = lsh