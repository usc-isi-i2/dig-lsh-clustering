#!/usr/bin/env python

import sys


def compute_list_similarity(list1, list2):
    similarity = float(len(set(list1) & set(list2)))/float(len(set(list1)))
    return similarity


def pick_cluster_mean(key_hashes):
    return key_hashes[0]


def print_similarity(key_hashes, lsh_key):
    mean_key_hash = pick_cluster_mean(key_hashes)
    (key1, hash_str1) = mean_key_hash.split("\t", 1)
    hash_arr1 = hash_str1.split("\t")
    for keyArr2 in key_hashes:
        (key2, hash_str2) = keyArr2.split("\t", 1)
        if key1 < key2:
            hash_arr2 = hash_str2.split("\t")
            if hash_arr1 != hash_arr2:
                score = compute_list_similarity(hash_arr1, hash_arr2)
            else:
                score = 1.0
            lsh_band = lsh_key[0:3]
            if score < 1.0 or lsh_band == "000":
                print '%s\t%s\t%s\t%s' % (lsh_key, key1, key2, str(score))

current_lsh = None
itemKey_hashes = []
word = None


for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    (lsh, key_hash) = line.split('\t', 1)

    # this IF-switch only works because HADOOP sorts map output
    # by key (here: word) before it is passed to the reducer
    if current_lsh == lsh:
        itemKey_hashes.append(key_hash)
    else:
        if current_lsh:
            if len(itemKey_hashes) > 1:
                print_similarity(itemKey_hashes, current_lsh)
            del itemKey_hashes[:]
        current_lsh = lsh
        itemKey_hashes.append(key_hash)

if len(itemKey_hashes) > 1:
    print_similarity(itemKey_hashes, current_lsh)

exit(0)


