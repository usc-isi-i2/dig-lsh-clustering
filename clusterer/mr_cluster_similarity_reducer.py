#!/usr/bin/env python

import sys

current_lsh = None
key_hashes = set()
word = None
cluster_counts = {}


def compute_list_similarity(list1, list2):
    #similarity = float(len(set(list1) & set(list2)))/float(len(set(list1)))
    list_len = len(list1)
    num_match = 0
    for i in range(0, list_len):
        if list[i] == list2[i]:
            num_match += 1
    similarity = float(num_match)/float(list_len)
    return similarity


def print_similarity(key_hash_strings, lsh_key):
    for keyArr1 in key_hash_strings:
        (key1, hash_str1) = keyArr1.split("\t", 1)
        hash1 = hash_str1.split("\t")
        for keyArr2 in key_hash_strings:
            (key2, hash_str2) = keyArr2.split("\t", 1)
            hash2 = hash_str2.split("\t")
            if key1 < key2:
                if hash_str1 != hash_str2:
                    score = compute_list_similarity(hash1, hash2)
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
            key_hashes.clear()
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


