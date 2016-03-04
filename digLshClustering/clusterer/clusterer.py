#!/usr/bin/env python

from pyspark import SparkContext
from optparse import OptionParser
import json
import hashlib
from pyspark import StorageLevel

class Clusterer:
    def __init__(self, **options):
        self.numPartitions = options.get("numPartitions",10)
        self.computeSimilarity = options.get("computeSimilarity",False)
        self.threshold = options.get("threshold",0.0)
        self.computeIdenticalClusters = options.get("computeIdenticalClusters",False)
        self.base = options.get("base","")
        self.topk = options.get("topk","3")
        self.candidates_name = options.get("candidatesName","candidates")
        print 'in clustering'
        print self.computeSimilarity,self.threshold,self.topk

    def perform(self,rdd):
        print self.computeSimilarity,self.threshold,self.base
        if len(self.base) > 0:
            base = sc.sequenceFile(self.base).mapValues(lambda x: json.loads(x))
            result = clusterer.compute_clusters_with_base(rdd, base)
        else:
            if self.computeIdenticalClusters is True:
                (key_clusterids, result) = clusterer.compute_identical_clusters(rdd)
            else:
                result = self.compute_clusters(rdd)
        result = self.output_json(result, self.topk, self.candidates_name)
        if self.computeIdenticalClusters is True:
            return (result,key_clusterids)
        else:
            return result


    def compute_clusters_with_base(self, data, base):
        lsh_clusters = data.join(base, self.numPartitions)
        clusters_with_dups = lsh_clusters.flatMap(lambda x: self.__output_clusters_with_base(x[0],list(x[1])))
        return clusters_with_dups.reduceByKey(lambda value1, value2: self.__remove_duplicates(value1, value2))

    def compute_clusters(self, data):
        lsh_clusters = data.groupByKey(self.numPartitions)
        clusters_with_dups = lsh_clusters.flatMap(lambda x: self.__output_cluster(x[0], list(x[1])))
        return clusters_with_dups.reduceByKey(lambda value1, value2: self.__remove_duplicates(value1, value2),numPartitions=self.numPartitions)

    def compute_identical_clusters(self, data):
        lsh_clusters = data.groupByKey(self.numPartitions)
        lsh_clusters.persist(StorageLevel.MEMORY_AND_DISK)
        key_clusters = lsh_clusters.flatMap(lambda x: self.__output_key_cluster_ids(x[0], list(x[1])))
        clusterid_clusters = lsh_clusters.flatMap(lambda x: self.__output_cluster_ids_minhash(x[0], list(x[1])))
        clusters_with_dups = clusterid_clusters.groupByKey().flatMap(lambda x: self.__output_cluster(x[0], list(x[1])))
        return key_clusters, self.__deduplicate_clusters(clusters_with_dups)

    def output_csv(self, data, top_k, separator):
        sorted_data = data.mapValues(lambda x: self.__sort_by_score(x))
        if self.computeSimilarity is True and top_k != -1:
            top_data = sorted_data.mapValues(lambda x: self.__get_top_k(x, top_k))
        else:
            top_data = sorted_data
        return top_data.flatMap(lambda x: list(self.__generate_csv(x[0], x[1], separator)))

    def output_json(self, data, top_k, candidates_name):
        sorted_data = data.mapValues(lambda x: self.__sort_by_score(x))
        if self.computeSimilarity is True and top_k != -1:
            top_data = sorted_data.mapValues(lambda x: self.__get_top_k(x, top_k))
        else:
            top_data = sorted_data
        return top_data.map(lambda x: self.__generate_json(x[0], x[1], candidates_name))

    def __sort_by_score(self, matches):
        return sorted(matches, key=lambda x: float(x[1]), reverse=True)

    def __get_top_k(self, sorted_matches, top_k):
        result = []
        current_score = 0
        for match in sorted_matches:
            score = match[1]
            if len(result) < top_k:
                result.append(match)
                current_score = score
            elif score == current_score:
                result.append(match)
            else:
                break
        return result

    def __generate_json(self, key, matches, candidates_name):
        if len(self.base) > 0:
            json_obj = {"uri": str(key), candidates_name:[]}
            for match in matches:
                # print "Match:", type(match), ", ", match
                candidate = {}
                if type(match) is list or type(match) is dict or type(match) is tuple:
                    candidate["uri"] = str(match[0])
                    candidate["score"] = match[1]
                else:
                    candidate["uri"] = str(match)
                json_obj[candidates_name].append(candidate)
        else:
            json_obj = {"member":[]}
            json_obj["member"].append({"uri":key})
            for match in matches:
                # print "Match:", type(match), ", ", match
                candidate = {}
                if type(match) is list or type(match) is dict or type(match) is tuple:
                    candidate["uri"] = str(match[0])
                    if self.computeSimilarity is True:
                        candidate["score"] = match[1]
                else:
                    candidate["uri"] = str(match)
                json_obj["member"].append(candidate)


        return key + "/cluster", json_obj


    def __generate_csv(self, key, matches, separator):
        if self.computeSimilarity:
            for match in matches:
                if type(match) is list or type(match) is dict or type(match) is tuple:
                    yield str(key) + separator + str(match[0]) + separator + str(match[1])
                else:
                    yield str(key) + separator + str(match)
        else:
            line = str(key)
            for match in matches:
                if type(match) is list or type(match) is dict or type(match) is tuple:
                    line = line + separator + str(match[0])
                else:
                    line = line + separator + str(match)
            yield line


    def __output_clusters_with_base(self, lsh_key, cluster):
        #print "Output clusters for key:", lsh_key
        data = cluster[0]
        key = data[0]
        matches = cluster[1:]
        for match in matches:
            match_key = match[0]
            if self.computeSimilarity is True:
                score = self.__compute_list_similarity_score(data[1], match[1])
                if self.threshold <= 0 or score >= self.threshold:
                    yield key, [(match_key, score)]
            else:
                yield key, [(match_key, 0)]

    def __output_cluster(self, lsh_key, cluster):
        if len(cluster) > 0 :
            for data in cluster:
                key1 = data[0]
                for match in cluster:
                    key2 = match[0]
                    if key1 < key2:
                        if self.computeSimilarity is True:
                            score = self.__compute_list_similarity_score(data[1], match[1])
                            if self.threshold <= 0 or score >= self.threshold:
                                yield key1, [(key2, score)]
                        else:
                            yield key1, [(key2, 0)]

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

    def __compute_list_similarity_score(self, list1, list2):
        similarity = float(len(set(list1) & set(list2)))/float(len(set(list1)))
        return similarity

    def __remove_duplicates(self, value1, value2):
        if value1 is None:
            return value2
        if value2 is None:
            return value1

        seen = list()
        for i in range(0, len(value1)):
            match = value1[i]
            seen.append(str(match[0]))

        for i in range(0, len(value2)):
            match = value2[i]
            key = str(match[0])
            if key in seen:
                idx = seen.index(key)

                score = value1[idx][1]
                this_score = match[1]

                if this_score > score:
                    value1[idx] = (key, this_score)
                continue
            #print "Added new:", key, "seen:", seen
            seen.append(key)
            value1.append(match)

        return value1

if __name__ == "__main__":
    """
        Usage: clusterer.py [input1] [input1Prefix] [base] [base2Prefix] [outputFilename]
    """
    sc = SparkContext(appName="LSH-CLUSTERER")

    usage = "usage: %prog [options] input1 input1Prefix <input2> <input2Prefix> output"
    parser = OptionParser()
    parser.add_option("-r", "--separator", dest="separator", type="string",
                      help="field separator", default="\t")

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
    parser.add_option("-y", "--outputtype", dest="outputtype", type="string",
                      help="output type: csv/json", default="json")
    parser.add_option("-k", "--topk", dest="topk", type="int",
                      help="top n matches", default=3)
    parser.add_option("-x", "--numPartitions", dest="numPartitions", type="int",
                      help="number of partitions", default=10)
    parser.add_option("-z", "--candidatesName", dest="candidates_name", type="string",
                        help="name for json element for matching candidates", default="candidates")
    (c_options, args) = parser.parse_args()
    print "Got options:", c_options

    inputFilename = args[0]
    outputFilename = args[1]
    print "Save to:", outputFilename

    clusterer = Clusterer(c_options.numPartitions,
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

    if c_options.outputtype == "json":
        result = clusterer.output_json(result, c_options.topk, c_options.candidates_name)
    else:
        result = clusterer.output_csv(result, c_options.topk, c_options.separator)

    if c_options.outputformat == "text":
        result.saveAsTextFile(outputFilename)
        if c_options.computeIdenticalClusters is True:
            key_clusterids.saveAsTextFile(outputFilename + "-key-clusterids")
    else:
        result.mapValues(lambda x: json.dumps(x)).saveAsSequenceFile(outputFilename)
        if c_options.computeIdenticalClusters is True:
            key_clusterids.mapValues(lambda x, y: json.dumps(x)).saveAsSequenceFile(outputFilename + "-key-clusterids")


