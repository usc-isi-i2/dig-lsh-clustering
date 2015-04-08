__author__ = 'dipsy'

import sys
from hasher.lsh.lsh import LSH, MinHashSignature

numHashes = 100
numItemsInBand = 10
keyPrefix = ""

hasher = LSH(numHashes, numItemsInBand, None)
signer = MinHashSignature(numHashes)

def write_tokens(tokens, sep):
    outStr = ""
    usep = ""
    for token in tokens:
        outStr = outStr + usep + str(token)
        usep = sep
    return outStr

numBands = numHashes/numItemsInBand
separator = "\t"
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    idx = line.find("\t")
    if idx != -1:
        key = line[0:idx]
        data = line[idx+1:]
        if len(data) > 0:
            #print "Data:" + data
            tokens = data.split(separator)
            if len(tokens) > 0:
                #print "Adding tokens: " + str(tokens)
                minHashSig = signer.sign(tokens)
                lshSig = list(hasher.hash(minHashSig))
                minOut = separator + write_tokens(minHashSig, separator)

                for i in range(0, numBands):
                    print (str(i).zfill(3) + ":" + lshSig[i] + separator + keyPrefix + key + minOut + "\n")

exit(0)
