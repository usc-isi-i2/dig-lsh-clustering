#!/usr/bin/env python

import sys

current_lsh = None
key_hashes = set()
word = None
cluster_counts = {}


def compute_list_similarity(str1, str2):
    similarity = float(len(str1 & str2))/float(len(str1))
    return similarity


def print_similarity(key_hashes, lsh_key):
    for keyArr1 in key_hashes:
        (key1, hash_str1) = keyArr1.split("\t", 1)
        for keyArr2 in key_hashes:
            (key2, hash_str2) = keyArr2.split("\t", 1)
            if key1 < key2:
                if hash_str1 != hash_str2:
                    score = compute_list_similarity(hash_str1, hash_str2)
                else:
                    score = 1.0
                lsh_band = lsh_key[0:3]
                if score < 1.0 or lsh_band == "000":
                    print '%s\t%s\t%s\t%s\t%s' % (key1, key2, str(score),
                                                  str(cluster_counts.get(key1)),
                                                  str(cluster_counts.get(key2))
                                                )

for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    (lsh, cluster_id, minhash) = line.split('\t', 2)

    # this IF-switch only works because HADOOP sorts map output
    # by key (here: word) before it is passed to the reducer
    if current_lsh == lsh:
        key_hashes.add(cluster_id + "\t" + minhash)
    else:
        if current_lsh:
            if len(key_hashes) > 1:
                print_similarity(key_hashes, current_lsh)
            del key_hashes[:]
        current_lsh = lsh
        key_hashes.add(cluster_id + "\t" + minhash)
    count = 0
    if cluster_counts.has_key(cluster_id):
        count = cluster_counts.get(cluster_id)
    count += 1
    cluster_counts[cluster_id] = count

if len(key_hashes) > 1:
    print_similarity(key_hashes, current_lsh)

exit(0)


