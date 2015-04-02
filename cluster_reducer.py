#!/usr/bin/env python

import sys

def compute_list_similarity(list1, list2):
    similarity = float(len(set(list1) & set(list2)))/float(len(set(list1)))
    return similarity


def print_similarity(itemkey_minhashes):
    for keyArr1 in itemKey_minhashes:
        (key1, minarrStr1) = keyArr1.split("\t", 1)
        minarr1 = minarrStr1.split("\t")
        for keyArr2 in itemKey_minhashes:
            (key2, minarrStr2) = keyArr2.split("\t", 1)
            if key1 < key2:
                minarr2 = minarrStr2.split("\t")
                if minarr1 != minarr2:
                    score = compute_list_similarity(minarr1, minarr2)
                else:
                    score = 1.0
                lsh_band = current_lsh[0:3]
                if score < 1.0 or lsh_band == "000":
                    print '%s\t%s\t%s' % (key1, key2, str(score))

current_lsh = None
itemKey_minhashes = []
word = None

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    (lsh, key_minhash) = line.split('\t', 1)

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if current_lsh == lsh:
        itemKey_minhashes.append(key_minhash)
    else:
        if current_lsh:
            # write result to STDOUT
            if len(itemKey_minhashes) > 1: 
	    	print_similarity(itemKey_minhashes)
            del itemKey_minhashes[:]
        current_lsh = lsh
        itemKey_minhashes.append(key_minhash)

# do not forget to output the last word if needed!
if len(itemKey_minhashes) > 1:
    print_similarity(itemKey_minhashes)

exit(0)


