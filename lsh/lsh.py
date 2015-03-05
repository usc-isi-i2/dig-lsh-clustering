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

    # random_ints = [507, 196,  79, 522, 738, 996, 115, 343, 727, 872, 102, 447, 547,
    #    700, 753, 503, 120, 171, 476, 151, 379, 719, 124, 944, 875, 145,
    #    376, 260, 526, 116, 708, 534, 609, 694,  17, 929, 291,  73, 397,
    #    418, 259, 633, 755,  83,  26, 491,  90,  45,  77, 265, 159, 490,
    #    829, 229,  25, 316, 245, 980,  71, 917, 395, 927,  79, 109, 979,
    #      8, 753, 251, 517, 941, 737, 971, 989,  35, 854, 892, 637,  87,
    #    476, 361, 400, 588, 506,  47, 494, 550, 968, 870, 972,  75, 608,
    #    820,  77,  22, 870, 327, 384, 271, 896, 562]

    #numpy.random.randint(1000000, 100000000, 100)
    random_ints = [51998748, 93498184, 61059477, 52880981,  2000959, 88381887,
       46178092,  1550718, 66906016, 48232123, 55763301, 37313922,
       38252277, 63070672, 36822158,  6190792, 45803061, 19626999,
       46362502,  1600956, 10454321, 26407057, 87269824, 37876222,
       55628313, 87251790, 49737164, 33558588, 13609187, 16200540,
       66596626, 87222234, 27570638, 67419278, 69969148, 23279986,
       24768567, 65892152, 58607293, 50723694, 89246437, 70746160,
       18072023,  3410393, 85347128, 31572599, 34635847, 64545199,
       48968049, 78670136, 65000284, 88960285, 94673106, 45489519,
       95135626, 25146681, 59725811, 20192197, 34504760, 27035030,
       50436063, 44755176, 62801560, 56177570, 15129140, 60171501,
       23899533, 37066545, 46872328, 74223180, 18272535, 75783866,
        9119632, 56121465, 54954553,  7570517, 75726161, 24884577,
       14152140, 15584712, 35875836,  6137412, 77035467, 81931741,
       33025578, 90241436, 28338233, 51815562, 91177556, 92823300,
       46084291, 52253969, 85771590, 37257347, 26132367, 43279296,
       60538475,  5659999, 76867186, 78184107]



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
            yield ''.join(str(x).zfill(10) for x in band)

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
        self.lshmap = {}

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
        self.lshmap[label] = []

        for band_idx, hshval in enumerate(lshKeys):
            #print "Got band_idx, hashval: " + str(band_idx) + "," + str(hshval)
            self.hashmaps[band_idx][hshval].append(label)
            self.unionfind.union(label, self.hashmaps[band_idx][hshval][0])
            self.lshmap[label].append(hshval)

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
                        if self.lshmap[label]:
                            list2.append((label, self.lshmap[label]))
                        else:
                            list2.append(label)
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
        self.lshmap = {}

    def add_set(self, s, label=None):
        # A label for this set
        if not label:
            label = s

        int_s = []
        for t in s:
            t = t.strip()
            if len(t) > 0:
                int_s.append(int(t))
        if len(int_s) > 0:
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


