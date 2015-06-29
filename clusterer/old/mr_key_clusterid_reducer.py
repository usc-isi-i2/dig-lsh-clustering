#!/usr/bin/env python

import sys


current_key = None
cluster_ids = set()
word = None


def print_output(key, cluster_id_arr):
    for cluster_id_val in cluster_id_arr:
        print "%s\t%s" % (key, cluster_id_val)


for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    (key, cluster_id) = line.split('\t', 1)

    # this IF-switch only works because HADOOP sorts map output
    # by key (here: word) before it is passed to the reducer
    if current_key == key:
        cluster_ids.add(cluster_id)
    else:
        if current_key:
            if len(cluster_ids) > 0:
                print_output(current_key, cluster_ids)
            cluster_ids.clear()

        current_key = key
        cluster_ids.add(cluster_id)

if len(cluster_ids) > 0:
    print_output(current_key, cluster_ids)

exit(0)
