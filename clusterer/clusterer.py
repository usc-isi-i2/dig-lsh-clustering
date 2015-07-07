#!/usr/bin/env python

from pyspark import SparkContext
from optparse import OptionParser
import json
import hashlib
from pyspark import StorageLevel

class Clusterer:
    def __init__(self, p_numPartitions, p_numHashes, p_numItemsInBand, p_computeSimilarity, p_threshold):
        self.numPartitions = p_numPartitions
        self.numHashes = p_numHashes
        self.numItemsInBand = p_numItemsInBand
        self.computeSimilarity = p_computeSimilarity
        self.threshold = p_threshold

    def compute_clusters_with_base(self, data, base):
        lsh_clusters = data.join(base, self.numPartitions)
        clusters_with_dups = lsh_clusters.flatMap(lambda x: self.__output_clusters_with_base(x[0],list(x[1])))
        return self.__deduplicate_clusters(clusters_with_dups)

    def compute_clusters(self, data):
        lsh_clusters = data.groupByKey(self.numPartitions)
        clusters_with_dups = lsh_clusters.flatMap(lambda x: self.__output_cluster(x[0], list(x[1])))
        return self.__deduplicate_clusters(clusters_with_dups)

    def compute_identical_clusters(self, data):
        lsh_clusters = data.groupByKey(self.numPartitions)
        lsh_clusters.persist(StorageLevel.MEMORY_AND_DISK)
        key_clusters = lsh_clusters.flatMap(lambda x: self.__output_key_cluster_ids(x[0], list(x[1])))
        clusterid_clusters = lsh_clusters.flatMap(lambda x: self.__output_cluster_ids_minhash(x[0], list(x[1])))
        clusters_with_dups = clusterid_clusters.groupByKey().flatMap(lambda x: self.__output_cluster(x[0], list(x[1])))
        return key_clusters, self.__deduplicate_clusters(clusters_with_dups)

    def __deduplicate_clusters(self, clusters_with_dups):
        clusters_no_dups = clusters_with_dups.reduceByKey(lambda value1, value2: self.__check_duplicate(value1, value2))
        return clusters_no_dups.flatMapValues(lambda x: self.__compute_similarity(x))

    def __output_clusters_with_base(self, lsh_key, cluster):
        #print "Output clusters for key:", lsh_key
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

    def __output_key_cluster_ids(self, lsh_key, cluster):
        if len(cluster) > 0 :
            for data in cluster:
                key = data[0]
                hashes = data[1]
                cluster_id = hashlib.sha1(",".join(hashes)).hexdigest()
                yield key, cluster_id

    def __output_cluster_ids_minhash(self, lsh_key, cluster):
        if len(cluster) > 0 :
            for data in cluster:
                key = data[0]
                hashes = data[1]
                cluster_id = hashlib.sha1(",".join(hashes)).hexdigest()
                yield lsh_key, (cluster_id, hashes)

    def __compute_similarity(self, matches):
        # print "Compite similarity:", matches

        for i in self.custom_range(0, len(matches)-1, 2):
            match = matches[i]
            hashes = matches[i+1]
            # print "Got match:", match
            # print "Got hashes:", hashes

            if self.computeSimilarity is True:
                key_minhash = hashes[0]
                match_hashes = hashes[1]
                score = self.__compute_list_similarity_score(key_minhash, match_hashes)
                if score > self.threshold:
                    yield (match, score)
            else:
                yield match

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

    def custom_range(self, start, end, step):
        while start <= end:
            yield start
            start += step

    def __check_duplicate(self, value1, value2):
        if value1 is None:
            return value2
        if value2 is None:
            return value1

        seen = list()
        for i in self.custom_range(0, len(value1)-1, 2):
            key = value1[i]
            #print "Added to see:", i, ":", key
            seen.append(key)


        for i in self.custom_range(0, len(value2)-1, 2):
            key = value2[i]
            if key in seen:
                continue
            seen.append(key)
            #print "Added new:", key
            value1.append(value2[i])
            value1.append(value2[i+1])

        return value1

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
    parser.add_option("-j", "--computeIdenticalClusters", action="store_true",
                      dest="computeIdenticalClusters", default=False, help="compute identical clusters")
    parser.add_option("-t", "--threshold", type="float",
                      dest="threshold", default=0.0, help="similarity threshold")
    parser.add_option("-e", "--base", dest="base", type="string",
                      help="base file", default="")
    parser.add_option("-o", "--outputformat", dest="outputformat", type="string",
                      help="output file format: text/sequence", default="text")
    parser.add_option("-x", "--numPartitions", dest="numPartitions", type="int",
                      help="number of partitions", default=1000)
    (c_options, args) = parser.parse_args()
    print "Got options:", c_options

    inputFilename = args[0]
    outputFilename = args[1]
    print "Save to:", outputFilename

    clusterer = Clusterer(c_options.numPartitions, c_options.numHashes, c_options.numItemsInBand,
                          c_options.computeSimilarity, c_options.threshold)
    rdd = sc.sequenceFile(inputFilename).mapValues(lambda x: json.loads(x))
    if len(c_options.base) > 0:
        base = sc.sequenceFile(c_options.base).mapValues(lambda x: json.loads(x))
        result = clusterer.compute_clusters_with_base(rdd, base)
    else:
        if c_options.computeIdenticalClusters is True:
            (key_clusterids, result) = clusterer.compute_identical_clusters(rdd)
        else:
            result = clusterer.compute_clusters(rdd)

    if c_options.outputformat == "text":
        result.saveAsTextFile(outputFilename)
        if c_options.computeIdenticalClusters is True:
            key_clusterids.saveAsTextFile(outputFilename + "-key-clusterids")
    else:
        result.mapValues(lambda x, y: json.dumps(x)).saveAsSequenceFile(outputFilename)
        if c_options.computeIdenticalClusters is True:
            key_clusterids.mapValues(lambda x, y: json.dumps(x)).saveAsSequenceFile(outputFilename + "-key-clusterids")


