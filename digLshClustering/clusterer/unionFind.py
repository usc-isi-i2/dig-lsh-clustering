#!/usr/bin/env python
from pyspark import SparkContext
from optparse import OptionParser
import json
from digSparkUtil.fileUtil import FileUtil

# Implementation Of: https://chasebradford.wordpress.com/2010/10/23/mapreduce-implementation-for-union-find/

def prepareMap(tuple):
    key = tuple[0]
    value = tuple[1]

    if key != "OPEN":
        value.append(key)

    # print "PREPARE:", value

    return "OPEN", value


def electMap(tuple):
    x = tuple[1]

    pick = str(x[0])
    for z in x[1:]:
        z = str(z)
        if z < pick:
            pick = z
    res = []
    for z in x:
        z = str(z)
        if z != pick:
            # print "ELECTMAP:", z, ",", [pick]
            yield z, [pick]
            res.append(z)

    # print "ELECTMAP:", pick, ",", res
    yield pick, res


def finalMap(tuple):
    # print "FINAL MAP:", tuple
    pick = str(tuple[0])
    x = tuple[1]

    for z in x:
        z = str(z)
        if z < pick:
            pick = z
    res = []
    for z in x:
        z = str(z)
        if z != pick:
            res.append(z)

    # print "FINALMAP RETURNS:", pick, ",", res
    return (pick, res)


def electReduce(val1, val2):
    # print "electReduce: v1:", val1, "v2:", val2
    x = set(val1)
    for z in val2:
        x.add(z)
    # print "ELECTREDUCE:", list(x)
    return list(x)


def partitionMap(tuple):
    key = tuple[0]
    value = tuple[1]
    # print "Got key:", key
    if len(value) > 1:
        # print "PARTITIONMAP:", key, ",", value
        return key, value
    else:
        # print "PARTITIONMAP:", value[0], ",", [key]
        return value[0], [key]


def partitionReduce(val1, val2):
    global OPEN
    counts = dict()
    for val in val1:
        counts[str(val)] = 1

    for val in val2:
        if str(val) in counts:
            counts[str(val)] = counts[str(val)] + 1
        else:
            counts[str(val)] = 1

    openValue = False
    res = set()
    for val in val1:
        if counts[str(val)] == 1:
            openValue = True
        res.add(val)

    for val in val2:
        if counts[str(val)] == 1:
            openValue = True
        res.add(val)

    # print "PARTITIONREDUCE:", list(res)
    return list(res)


def partitionReduceOpen(val1, val2):
    # print "partitionReduceOpen:", val1, " :: ", val2
    val1.extend(val2)
    return val1


def compute_sum(rdd1, rdd2):
    # print "Compute_sum", rdd1[0], ":", rdd1[1], " AND ", rdd2[0], ":", rdd2[1]
    sum1 = 0
    if rdd1[1]:
        sum1 = check_if_open(rdd1[1])
    sum2 = 0
    if rdd2[1]:
        sum2 = check_if_open(rdd2[1])
    # print "Sum:", sum1, "+", sum2, "=", (sum1+sum2)
    return sum1 + sum2


def check_if_open(tuple):
    arr = tuple[1]
    counts = dict()
    for val in arr:
        if str(val) in counts:
            counts[str(val)] += 1
        else:
            counts[str(val)] = 1

    for val in arr:
        if counts[str(val)] != 2:
            return 1

    return 0


class UnionFind:
    def __init__(self, **options):
        self.sums = 0
        self.prevSums = -1
        self.numTries = 0
        self.options = options
        print 'in unionFind'



    def perform(self, rdd):
        rdd = self.read_input(rdd)
        while True:
            rdd = self.run(rdd)
            if self.isEnd():
                break
        rdd = self.format_output(rdd)
        return rdd

    def output_csv(self, key, matches, separator):
        line = str(key)
        for match in matches:
            if type(match) is list or type(match) is dict or type(match) is tuple:
                line = line + separator + str(match[0])
            else:
                line = line + separator + str(match)
        yield line

    def read_input(self, rdd):
        def parse_json(tuple):
            x = tuple[1]
            ##if it's in the form of rdd x will be a dictionary else it will be string
            if isinstance(x,dict):
                cluster = x["member"]
            else:
                cluster = json.loads(x)["member"]
            res = []
            for item in cluster:
                res.append(item["uri"])
            return res

        rdd = rdd.map(lambda x: ("OPEN", parse_json(x)))
        return rdd

    def format_output(self, rdd):
        rdd_map = rdd.map(finalMap)
        rdd_final = rdd_map.reduceByKey(electReduce)

        def save_as_json(tuple):
            key = tuple[0]
            json_obj = {"member": [],"a":"http://schema.dig.isi.edu/ontology/Cluster"}
            json_obj["member"].append({"uri": key,"a":"http://schema.org/WebPage"})
            #json_obj["uri"]=key
            concat_str =''
            for val in tuple[1]:
                json_obj["member"].append({"uri": val,"a":"http://schema.org/WebPage"})
                concat_str += val
            cluster_id = "http://dig.isi.edu/ht/data/" + str(hash(concat_str)% 982451653)
            json_obj["uri"]=cluster_id
            return cluster_id + "/cluster", json_obj

        rdd_final = rdd_final.map(save_as_json)
        return rdd_final

    def run(self, rdd):
        process_rdd = rdd.map(prepareMap)
        rdd_elect_map = process_rdd.flatMap(electMap)
        rdd_elect_reduce = rdd_elect_map.reduceByKey(electReduce)
        rdd_partition_map = rdd_elect_reduce.map(partitionMap)
        self.sums = rdd_partition_map.reduceByKey(partitionReduceOpen).map(check_if_open).sum()
        print "Got sums:", self.sums

        rdd_partition_reduce = rdd_partition_map.reduceByKey(partitionReduce)
        # print "RDD:", rdd_partition_reduce.collect()
        return rdd_partition_reduce

    def isEnd(self):
        # print "============================================================"
        end = False
        if self.sums == 0:
            end = True
        elif self.prevSums == self.sums:
            self.numTries += 1
            if self.numTries >= 4:
                end = True
        else:
            self.numTries = 0

        self.prevSums = self.sums

        return end

'''
if __name__ == "__main__":
    sc = SparkContext(appName="LSH-UNION")
    parser = OptionParser()
    parser.add_option("-r", "--separator", dest="separator", type="string",
                      help="field separator", default="\t")
    parser.add_option("-i", "--inputformat", dest="inputformat", type="string",
                      help="input file format: text/sequence", default="text")
    parser.add_option("-t", "--inputtype", dest="inputtype", type="string",
                      help="output type: csv/json", default="json")
    parser.add_option("-o", "--outputformat", dest="outputformat", type="string",
                      help="output file format: text/sequence", default="text")
    parser.add_option("-y", "--outputtype", dest="outputtype", type="string",
                      help="output type: csv/json", default="json")

    (c_options, args) = parser.parse_args()
    inputFilename = args[0]
    outputFilename = args[1]

    unionFind = UnionFind()
    rdd = unionFind.read_input(inputFilename, c_options)

    while True:
        rdd = unionFind.run(rdd)
        if unionFind.isEnd():
            break
    # x = rdd_elect_reduce.flatMap(lambda key, value: unionFind.output_csv(key, value, c_options.separator))
    unionFind.save_output(rdd, outputFilename, c_options)
'''