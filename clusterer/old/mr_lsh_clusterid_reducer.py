#!/usr/bin/env python

import sys


current_lsh_clusterid = None
cluster_count = 1
current_minhash = None

for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    (lsh_cluster_id, minhash) = line.split('\t', 1)

    # this IF-switch only works because HADOOP sorts map output
    # by key (here: word) before it is passed to the reducer
    if current_lsh_clusterid == lsh_cluster_id:
        cluster_count += 1
    else:
        if current_lsh_clusterid:
            (lsh, cluster_id) = current_lsh_clusterid.split("$$$$", 1)
            print "%s\t%s\t%s\t%s" % (lsh, cluster_id, cluster_count, current_minhash)
        cluster_count = 1
        current_lsh_clusterid = lsh_cluster_id
        current_minhash = minhash

if current_lsh_clusterid:
    (lsh, cluster_id) = current_lsh_clusterid.split("$$$$", 1)
    print "%s\t%s\t%s\t%s" % (lsh, cluster_id, cluster_count, current_minhash)
