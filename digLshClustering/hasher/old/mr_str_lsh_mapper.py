#!/usr/bin/env python

import sys
import hashlib

numHashes = 100
numItemsInBand = 10
keyPrefix = ""

class Signature(object):
    """Signature Base class."""

    def __init__(self, dim):
        self.dim = dim
        self.hashes = self.hash_functions()

    def hash_functions(self):
        """Returns dim different hash functions"""
        pass

    def sign(self, object):
        """Return the signature for object s"""
        pass


class MinHashSignature(Signature):
    """Creates signatures for sets/tuples using minhash."""

    def hash_functions(self):
        """Return dim different hash functions"""
        def hash_factory(n):
            return lambda x: hashlib.sha1(str(n) + x).hexdigest()
             #return lambda x: hash("salt" + unicode(n) + unicode(x) + "salt")

        return [ hash_factory(_) for _ in range(self.dim) ]

    def sign(self, s):
        """Returns minhash signature for set s"""
        sig = [ float("inf") ] * self.dim
        for hash_ix, hash_fn in enumerate(self.hashes):
            sig[hash_ix] = min(hash_fn(value) for value in s)
            #sig[hash_ix] = min(self.callHashFunc(hash_fn, value) for value in s)
        return sig

    def callHashFunc(self, func, value):
        value = func(value)


class LSH(object):
    """Locality sensitive hashing.  Uses a banding approach to hash
    similar signatures to the same buckets."""
    def __init__(self, n, r, t):
        self.n = n
        self.r = r
        self.b = int(self.n / self.r)
        if t:
            self.t = t
        else:
            self.t = self.get_threshold()
        #print "Setting threshold=" + str(self.t)

    def hash(self, sig):
        """Generate hashvals for this signature"""
        for band in zip(*(iter(sig),) * self.r):
            #band = r values from sig at a time
            yield ''.join(list(band))
            #yield hashlib.sha1("salt" + unicode(band) + "tlas").hexdigest()
            #yield hash("salt" + unicode(band) + "tlas")


    def get_threshold(self):
        r = self.r
        b = self.b
        return (1. / b) ** (1. / r)

    def get_n_bands(self):
        return self.b

hasher = LSH(numHashes, numItemsInBand, None)
signer = MinHashSignature(numHashes)
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
                minOut = '\t'.join(minHashSig)

                for i in range(0, numBands):
                    print (str(i).zfill(3) + ":" + lshSig[i] + separator +
                           keyPrefix + key + separator + minOut)

exit(0)
