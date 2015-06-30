#!/usr/bin/env python

from pyspark import SparkContext
from optparse import OptionParser
import json


class Clusterer:
    def __init__(self, p_numHashes, p_numItemsInBand, p_computeSimilarity, p_threshold):
        self.numHashes = p_numHashes
        self.numItemsInBand = p_numItemsInBand
        self.computeSimilarity = p_computeSimilarity
        self.threshold = p_threshold
        pass

    def compute_clusters_with_base(self, data, base):
        lsh_clusters = data.join(base)
        clusters_with_dups = lsh_clusters.flatMap(lambda x: self.__output_clusters_with_base(x[0],list(x[1])))
        return self.__deduplicate_clusters(clusters_with_dups)

    def compute_clusters(self, data):
        lsh_clusters = data.groupByKey()
        clusters_with_dups = lsh_clusters.flatMap(lambda x: self.__output_cluster(x[0], list(x[1])))
        return self.__deduplicate_clusters(clusters_with_dups)

    def __deduplicate_clusters(self, clusters_with_dups):
        clusters_no_dups = clusters_with_dups.groupByKey().mapValues(lambda x:
                                                                          list(self.__remove_duplicates(list(x)))
        )
        return clusters_no_dups.flatMapValues(lambda x: self.__compute_similarity(x))

    def __output_clusters_with_base(self, lsh_key, cluster):
        data = cluster[0]
        key = data[0]
        matches = cluster[1:]
        for match in matches:
            arr_hashes = [data[1], match[1]]
            match_with_data = [match[0], arr_hashes]
            yield key, match_with_data

    def __output_cluster(self, lsh_key, cluster):
        if len(cluster) > 0 :
            for data in cluster:
                key1 = data[0]
                for match in cluster:
                    key2 = match[0]
                    if key1 < key2:
                        arr_hashes = [data[1], match[1]]
                        match_with_data = [match[0], arr_hashes]
                        yield key1, match_with_data

    def __compute_similarity(self, matches):
        for match in matches:
            if self.computeSimilarity is True:
                key_minhash = match[1][0]
                match_hashes = match[1][1]
                #print "Got match", match_hashes
                #print "Got minhash:", key_minhash
                score = self.__compute_list_similarity_score(key_minhash, match_hashes)
                yield (match[0], score)
            else:
                yield match[0]

    def __compute_list_similarity_score(self, list1, list2):
        similarity = float(len(set(list1) & set(list2)))/float(len(set(list1)))
        return similarity

    def __remove_duplicates(self, list_data):
        seen = list()
        list_data = list_data[1:]
        for x in list_data:
            if x[0] in seen:
                continue
            else:
                seen.append(x[0])
                yield x

if __name__ == "__main__":
    """
        Usage: clusterer.py [input1] [input1Prefix] [base] [base2Prefix] [outputFilename]
    """
    sc = SparkContext(appName="LSH-CLUSTERER")

    usage = "usage: %prog [options] input1 input1Prefix <input2> <input2Prefix> output"
    parser = OptionParser()
    parser.add_option("-n", "--numHashes", dest="numHashes", type="int",
                      help="number of minhashes", default=100)
    parser.add_option("-b", "--numItemsInBand", dest="numItemsInBand", type="int",
                      help="number of items in each band", default=10)
    parser.add_option("-s", "--computeSimilarity", action="store_true",
                      dest="computeSimilarity", default=False, help="compute similarity")
    parser.add_option("-t", "--threshold", type="float",
                      dest="threshold", default=0.0, help="similarity threshold")
    parser.add_option("-e", "--base", dest="base", type="string",
                      help="base file", default="")
    parser.add_option("-o", "--outputformat", dest="outputformat", type="string",
                      help="output file format: text/sequence", default="text")

    (c_options, args) = parser.parse_args()
    print "Got options:", c_options

    inputFilename = args[0]
    outputFilename = args[1]
    print "Save to:", outputFilename

    clusterer = Clusterer(c_options.numHashes, c_options.numItemsInBand,
                          c_options.computeSimilarity, c_options.threshold)
    rdd = sc.sequenceFile(inputFilename).mapValues(lambda x: json.loads(x))
    if len(c_options.base) > 0:
        base = sc.sequenceFile(c_options.base).mapValues(lambda x: json.loads(x))
        result = clusterer.compute_clusters_with_base(rdd, base)
    else:
        result = clusterer.compute_clusters(rdd)

    if c_options.outputformat == "text":
        result.saveAsTextFile(outputFilename)
    else:
        result.mapValues(lambda x, y: json.dumps(x)).saveAsSequenceFile(outputFilename)
