"""
lsh.py

Algorithms based on 'Mining of Massive Datasets'
"""

from collections import defaultdict

from unionfind import UnionFind
import hashlib
#import numpy

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
        print "Called fun:" + str(func) + " --> " + str(value)

class IntegerMinHashSignature(MinHashSignature):
    """Creates signatures for sets/tuples using minhash."""
    #100 random ints generated using: numpy.random.randint(1, 1000, 100)

    random_ints = [507, 196,  79, 522, 738, 996, 115, 343, 727, 872, 102, 447, 547,
       700, 753, 503, 120, 171, 476, 151, 379, 719, 124, 944, 875, 145,
       376, 260, 526, 116, 708, 534, 609, 694,  17, 929, 291,  73, 397,
       418, 259, 633, 755,  83,  26, 491,  90,  45,  77, 265, 159, 490,
       829, 229,  25, 316, 245, 980,  71, 917, 395, 927,  79, 109, 979,
         8, 753, 251, 517, 941, 737, 971, 989,  35, 854, 892, 637,  87,
       476, 361, 400, 588, 506,  47, 494, 550, 968, 870, 972,  75, 608,
       820,  77,  22, 870, 327, 384, 271, 896, 562]

    #def __init__(self, dim):
    #    self.random_ints = numpy.random.randint(1, 100*dim, dim)
    #    MinHashSignature.__init__(self, dim)

    def hash_functions(self):
        """Return dim different hash functions"""
        def hash_factory(n, random_ints):
            return lambda x: (((random_ints[n] + 1) * x) + 1) % 982451653
            #return lambda x: (((2*n + 1)*x) + 1) % 982451653

        return [ hash_factory(_, self.random_ints) for _ in range(self.dim) ]


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

class IntegerLSH(LSH):
    def hash(self, sig):
        """Generate hashvals for this signature"""
        for band in zip(*(iter(sig),) * self.r):
            #band = r values from sig at a time
            yield ''.join(list(str(band)))

class Cluster(object):
    """Clusters sets with Jaccard similarity above threshold with high
    probability.

    Algorithm based on Rajaraman, "Mining of Massive Datasets":
    1. Generate set signature
    2. Use LSH to map similar signatures to same buckets
    3. Use UnionFind to merge buckets containing same values
    """
    def __init__(self, minHashLen=13, numRowsInBucket=2, threshold=None):
        self.unionfind = UnionFind()
        self.signer = MinHashSignature(minHashLen)
        self.hasher = LSH(minHashLen, numRowsInBucket, threshold)
        self.hashmaps = [defaultdict(list)
                         for _ in range(self.hasher.get_n_bands())]

    def add_set(self, s, label=None):
        # A label for this set
        if not label:
            label = s

        # Add to unionfind structure
        self.unionfind[label]

        # Get signature
        sig = self.signer.sign(s)

        # Union labels with same LSH key in same band
        lshKeys = self.hasher.hash(sig)

        for band_idx, hshval in enumerate(lshKeys):
            #print "Got band_idx, hashval: " + str(band_idx) + "," + str(hshval)
            self.hashmaps[band_idx][hshval].append(label)
            self.unionfind.union(label, self.hashmaps[band_idx][hshval][0])

    def get_clusters(self, min_cluster_len):
        for band_idx in range(0,len(self.hashmaps)):
            #print "clusters>Got band_idx: " + str(band_idx)
            hashmap = self.hashmaps[band_idx]
            for key in hashmap:
                list = hashmap[key]
                if(len(list) > min_cluster_len):
                    yield list

    def get_clusters_with_hashes(self, min_cluster_len):
        for band_idx in range(0,len(self.hashmaps)):
            hashmap = self.hashmaps[band_idx]
            for key in hashmap:
                list = hashmap[key]
                if(len(list) > min_cluster_len):
                    list2 = []
                    for label in list:
                       list2.append((label, self.lshMap[label]))
                    yield list2

    def get_cluster_unions(self, min_cluster_len):
        x = self.unionfind.sets()
        for set in x:
            if len(set) > min_cluster_len:
                yield set

    def get_min_hash(self, object):
        return list(self.signer.sign(object))

    def get_lsh_hash(self, object):
        sig = self.signer.sign(object)
        return list(self.hasher.hash(sig))


class IntegerCluster(Cluster):
    def __init__(self, minHashLen=13, numRowsInBucket=2, threshold=None):
        self.unionfind = UnionFind()
        self.signer = IntegerMinHashSignature(minHashLen)
        self.hasher = IntegerLSH(minHashLen, numRowsInBucket, threshold)
        self.hashmaps = [defaultdict(list)
                         for _ in range(self.hasher.get_n_bands())]

    def add_set(self, s, label=None):
        # A label for this set
        if not label:
            label = s

        int_s = []
        for t in s:
            int_s.append(int(t))
        Cluster.add_set(self, int_s, label)

def shingle(s, k):
    """Generate k-length shingles of string s"""
    k = min(len(s), k)
    for i in range(len(s) - k + 1):
        yield s[i:i+k]


def hshingle(s, k):
    """Generate k-length shingles then hash"""
    for s in shingle(s, k):
        yield hash(s)


def jaccard_sim(X, Y):
    """Jaccard similarity between two sets"""
    x = set(X)
    y = set(Y)
    return float(len(x & y)) / len(x | y)


def jaccard_dist(X, Y):
    """Jaccard distance between two sets"""
    return 1 - jaccard_sim(X, Y)


