#!/usr/bin/env python

from optparse import OptionParser
import json

from pyspark import SparkContext
from lsh.lsh import MinHashSignature
from lsh.lsh import LSH


class Hasher:
    def __init__(self, num_hashes, num_items_in_band, save_min_hash):
        self.signer = MinHashSignature(num_hashes)
        self.hasher = LSH(num_hashes, num_items_in_band, None)
        self.save_min_hash = save_min_hash
        pass

    def compute_hashes(self, data):
        return data.flatMap(lambda (x, y): self.compute_row_lsh(x, y))

    def compute_row_lsh(self, key, row):
        if len(row) > 0:
            #print "Sign:", row
            min_hash_sig = self.signer.sign(row)
            if min_hash_sig is not None:
                lsh_sig = list(self.hasher.hash(min_hash_sig))
                if self.save_min_hash is False:
                    min_hash_sig = None
                for lsh_val in lsh_sig:
                    yield lsh_val, (key, min_hash_sig)


if __name__ == "__main__":
    """
        Usage: hasher.py [input] [output]
    """
    sc = SparkContext(appName="LSH-HASHER")

    usage = "usage: %prog [options] input output"
    parser = OptionParser()
    parser.add_option("-n", "--numHashes", dest="numHashes", type="int",
                      help="number of minhashes", default=100)
    parser.add_option("-b", "--numItemsInBand", dest="numItemsInBand", type="int",
                      help="number of items in each band", default=10)
    parser.add_option("-s", "--saveMinhashes", action="store_true",
                      dest="saveMinhashes", default=False, help="save minhashes")

    (c_options, args) = parser.parse_args()
    print "Got options:", c_options
    inputFilename = args[0]
    outputFilename = args[1]

    hasher = Hasher(c_options.numHashes, c_options.numItemsInBand, c_options.saveMinhashes)
    rdd = sc.sequenceFile(inputFilename).mapValues(lambda x: json.loads(x))
    result = hasher.compute_hashes(rdd)
    result.mapValues(lambda x: json.dumps(x)).saveAsSequenceFile(outputFilename)
