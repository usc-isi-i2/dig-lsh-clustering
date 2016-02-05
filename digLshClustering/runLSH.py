#!/usr/bin/env python

from pyspark import SparkContext

from optparse import OptionParser
from tokenizer.tokenizer import Tokenizer
from hasher.hasher import Hasher
from clusterer.clusterer import Clusterer
import json

if __name__ == "__main__":
    """
        Usage: tokenizer.py [input] [output] [reducer:feature_name]...
    """
    sc = SparkContext(appName="LSH")

    usage = "usage: %prog [options] input config output"
    parser = OptionParser()
    parser.add_option("-r", "--separator", dest="separator", type="string",
                      help="field separator", default="\t")
    parser.add_option("-d", "--type", dest="data_type", type="string",
                      help="input data type: csv/json", default="csv")
    parser.add_option("-i", "--inputformat", dest="inputformat", type="string",
                      help="input file format: text/sequence", default="text")
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
    parser.add_option("-c", "--baseConfig", dest="baseConfig", type="string",
                      help="base config file", default="")
    parser.add_option("-o", "--outputformat", dest="outputformat", type="string",
                      help="output file format: text/sequence", default="text")
    parser.add_option("-x", "--numPartitions", dest="numPartitions", type="int",
                      help="number of partitions", default=10)
    parser.add_option("-y", "--outputtype", dest="outputtype", type="string",
                      help="output type: csv/json", default="json")
    parser.add_option("-k", "--topk", dest="topk", type="int",
                      help="top n matches", default=3)
    parser.add_option("-z", "--candidatesName", dest="candidates_name", type="string",
                        help="name for json element for matching candidates", default="candidates")

    (c_options, args) = parser.parse_args()
    print "Got options:", c_options
    inputFilename = args[0]
    configFilename = args[1]
    outputFilename = args[2]

    tokenizer = Tokenizer(configFilename, c_options)
    if c_options.inputformat == "text":
        rdd = tokenizer.tokenize_text_file(sc, inputFilename, c_options.data_type)
    else:
        rdd = tokenizer.tokenize_seq_file(sc, inputFilename, c_options.data_type)
    rdd.partitionBy(c_options.numPartitions)

    hasher = Hasher(c_options.numHashes, c_options.numItemsInBand, c_options.computeSimilarity)
    input_lsh_rdd = hasher.compute_hashes(rdd)

    clusterer = Clusterer(c_options.numPartitions,
                          c_options.computeSimilarity, c_options.threshold)

    if len(c_options.base) > 0:
        if len(c_options.baseConfig) > 0:
            tokenizer = Tokenizer(c_options.baseConfig, c_options)
        if c_options.inputformat == "text":
            base_rdd = tokenizer.tokenize_text_file(sc, c_options.base, c_options.data_type)
        else:
            base_rdd = tokenizer.tokenize_seq_file(sc, c_options.base, c_options.data_type)
        base_rdd.partitionBy(c_options.numPartitions)

        base_lsh_rdd = hasher.compute_hashes(base_rdd)
        result = clusterer.compute_clusters_with_base(input_lsh_rdd, base_lsh_rdd)
    else:
        if c_options.computeIdenticalClusters is True:
            (key_clusterids, result) = clusterer.compute_identical_clusters(input_lsh_rdd)
        else:
            result = clusterer.compute_clusters(input_lsh_rdd)

    if c_options.outputtype == "json":
        result = clusterer.output_json(result, c_options.topk, c_options.candidates_name)
    else:
        result = clusterer.output_csv(result, c_options.topk, c_options.separator)

    if c_options.outputformat == "text":
        if c_options.computeIdenticalClusters is True:
            key_clusterids.saveAsTextFile(outputFilename + "-key-clusterids")
        result.saveAsTextFile(outputFilename)
    else:
        if c_options.computeIdenticalClusters is True:
            key_clusterids.mapValues(lambda x, y: json.dumps(x)).saveAsSequenceFile(outputFilename + "-key-clusterids")
        result.mapValues(lambda x, y: json.dumps(x)).saveAsSequenceFile(outputFilename)
