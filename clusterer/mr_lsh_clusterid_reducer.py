#!/usr/bin/env python

import sys


current_lsh_clusterid = None

for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    (lsh_cluster_id, minhash) = line.split('\t', 1)

    # this IF-switch only works because HADOOP sorts map output
    # by key (here: word) before it is passed to the reducer
    if current_lsh_clusterid == lsh_cluster_id:
        continue
    (lsh, cluster_id) = lsh_cluster_id.split("$$$$", 1)
    print "%s\t%s\t%s" % (lsh, cluster_id, minhash)
    current_lsh_clusterid = lsh_cluster_id
