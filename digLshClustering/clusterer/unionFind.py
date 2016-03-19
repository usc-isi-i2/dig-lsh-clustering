#!/usr/bin/env python
from pyspark import SparkContext, StorageLevel, SparkConf
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
    key = tuple[0]
    if key == "OPEN":
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
                yield z, [pick]
            res.append(z)

        yield pick, res


def finalMap(tuple):
    # print "FINAL MAP:", tuple
    # pick = str(tuple[0])
    x = tuple[1]
    pick = x[0]
    for z in x:
        z = str(z)
        if z < pick:
            pick = z
    res = []
    for z in x:
        z = str(z)
        if z != pick:
            res.append(z)

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


def partitionReduceMap(tuple):
    arr = tuple[1]
    key = "DISJOINT"
    res = list()
    for x in arr:
        if x[1] != 2:
            key = "OPEN"
            break

    for x in arr:
        res.append(x[0])
    return key, res


def check_if_open(tuple):
    arr = tuple[1]
    for val in arr:
        if val[1] != 2:
            return 1
    return 0


class UnionFind:
    def __init__(self, **options):
        self.sums = 0
        self.prevSums = -1
        self.numTries = 0
        self.options = options
        self.numPartitions = options.get("numPartitions", -1)
        self.input_prefix = options.get("inputPrefix", None)
        self.numIterations = options.get("numIterations", -1)
        self.addToCluster = options.get("addToCluster",None)
        self.addToMember = options.get("addToMember",None)

    def perform(self, rdd):
        rdd = self.read_input(rdd)

        rdd = rdd.map(prepareMap)
        num = 0

        rdd_out = None
        while True:
            new_rdd = self.run(rdd, self.numPartitions)
            disjoint_rdd = new_rdd.filter(lambda x: x[0] == "DISJOINT")
            if rdd_out is None:
                rdd_out = disjoint_rdd
            else:
                rdd_out = rdd_out.union(disjoint_rdd)

            if self.numPartitions > 0:
                rdd_out = rdd_out.coalesce(self.numPartitions)

            rdd = new_rdd.filter(lambda x: x[0] == "OPEN")

            if self.isEnd():
                break

            num = num + 1
            if self.numIterations > 0 and num >= self.numIterations:
                break

        if rdd_out is not None:
            rdd_out = rdd_out.union(rdd)
        else:
            rdd_out = rdd
        return self.format_output(rdd_out)

    def output_csv(self, key, matches, separator):
        line = str(key)
        for match in matches:
            if type(match) is list or type(match) is dict or type(match) is tuple:
                line = line + separator + str(match[0])
            else:
                line = line + separator + str(match)
        yield line

    def read_input(self, rdd):
        def parse_json(tuple, prefix):
            x = tuple[1]

            ##if it's in the form of rdd x will be a dictionary else it will be string
            if isinstance(x, dict):
                cluster = x["member"]
            else:
                cluster = json.loads(x)["member"]
            res = []
            for item in cluster:
                uri = item["uri"]
                if prefix is not None:
                    idx = uri.rfind("/")
                    if idx != -1:
                        uri = uri[idx + 1:]
                res.append(uri)
            return res

        rdd = rdd.map(lambda x: ("OPEN", parse_json(x, self.input_prefix)))
        return rdd

    def format_output(self, rdd):
        rdd_map = rdd.map(finalMap)
        rdd_final = rdd_map.reduceByKey(electReduce)
        prefix = self.input_prefix
        if prefix is None:
            prefix = ""


        def save_as_json(prefix, tuple):
            key = tuple[0]
            if len(tuple[1])>1:
                json_obj = {}
                json_obj['member']=[]
                concat_str = ''
                for val in tuple[1]:
                    val = prefix + val
                    member_obj = {'uri':val}
                    if self.addToMember is not None:
                        member_obj['a']=self.addToMember
                    json_obj['member'].append(member_obj)
                    concat_str += val
                cluster_id = "http://dig.isi.edu/ht/data/" + str(hash(concat_str) % 982451653)
                json_obj['uri'] = cluster_id
                if self.addToCluster is not None:
                    json_obj['a'] = self.addToCluster

                return cluster_id + "/cluster",json_obj

        rdd_final = rdd_final.map(lambda x: save_as_json(prefix, x))
        rdd_final = rdd_final.filter(lambda x : x is not None)
        return rdd_final

    def run(self, rdd, numPartitions):
        # print "====================================="

        rdd_elect_map = rdd.flatMap(electMap)
        # for x in rdd_elect_map.collect():
        #     print "ELECT MAP:", x
        if numPartitions > 0:
            rdd_elect_reduce = rdd_elect_map.reduceByKey(electReduce, numPartitions)
        else:
            rdd_elect_reduce = rdd_elect_map.reduceByKey(electReduce)
        # for x in rdd_elect_reduce.collect():
        #     print "ELECT REDUCE:", x

        rdd_partition_map = rdd_elect_reduce.map(partitionMap)

        # for x in rdd_partition_map.collect():
        #     print "PARTITION MAP:", x

        def merge_arrays(arr, vals):
            # print "Merge:", arr, " with", vals
            for val in vals:
                if type(val) == list:
                    comp = val[0]
                else:
                     comp = val
                found = False
                for x in arr:
                    if x[0] == comp:
                        found = True
                        x[1] += 1
                        break
                if found is False:
                    arr.append([comp, 1])


            # print "Merge return:", arr
            return arr

        def merge_key(key, arr):
            # print "Merge:", arr, " with", vals

            found = False
            for x in arr:
                if x[0] == key:
                    found = True
                    x[1] += 1
                    break
            if found is False:
                arr.append([key, 2])


            # print "Merge return:", arr
            return key, arr

        def enlist_array(arr):
            # print "enlist", arr
            res = list()
            for x in arr:
                res.append([x, 1])
            # print "Enlist return:", res
            return res



        if numPartitions > 0:
            combine = rdd_partition_map.combineByKey((lambda x: enlist_array(x)),
                                                     (lambda x, y: merge_arrays(x, y)),
                                                     (lambda x, y: merge_arrays(x, y)),
                                                     numPartitions
            ).map(lambda x: merge_key(x[0], x[1]))
        else:
            combine = rdd_partition_map.combineByKey((lambda x: enlist_array(x)),
                                                     (lambda x, y: merge_arrays(x, y)),
                                                     (lambda x, y: merge_arrays(x, y))
            ).map(lambda x: merge_key(x[0], x[1]))
        # combine.persist(StorageLevel.MEMORY_AND_DISK)

        the_sums = combine.map(check_if_open).reduce(lambda x, y: x + y)
        print "Got sums:", the_sums


        # for x in combine.collect():
        #     print "Combine:", x

        rdd_partition_reduce = combine.map(partitionReduceMap)

        self.sums = the_sums
        # combine.unpersist()


        # for y in rdd_partition_reduce.collect():
        #     print "PARTITION REDUCE:", y

        # print "RDD:", rdd_partition_reduce.collect()
        return rdd_partition_reduce

    def isEnd(self):
        # print "============================================================"
        end = False
        if self.sums == 0:
            end = True
        elif self.prevSums == self.sums:
            self.numTries += 1
            if self.numTries >= 10:
                end = True
        else:
            self.numTries = 0

        self.prevSums = self.sums

        return end


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
    # inputFilename = args[0]
    # outputFilename = args[1]

    kwargs = {
        "numPartitions": 1,
        "inputPrefix": "",
        "numIterations": -1
    }

    unionFind = UnionFind(**kwargs)
    # input = [
    #         {"member": [{"uri":"http://dig.isi.edu/ht/data/webpage/A"}, {"uri": "http://dig.isi.edu/ht/data/webpage/B"}, {"uri": "http://dig.isi.edu/ht/data/webpage/C"}]},
    #         {"member": [{"uri":"http://dig.isi.edu/ht/data/webpage/A"}, {"uri": "http://dig.isi.edu/ht/data/webpage/D"}]},
    #         {"member": [{"uri":"http://dig.isi.edu/ht/data/webpage/C"}, {"uri": "http://dig.isi.edu/ht/data/webpage/E"}]},
    #         {"member": [{"uri":"http://dig.isi.edu/ht/data/webpage/F"}, {"uri": "http://dig.isi.edu/ht/data/webpage/G"}]},
    #         {"member": [{"uri":"http://dig.isi.edu/ht/data/webpage/G"}, {"uri": "http://dig.isi.edu/ht/data/webpage/H"}]},
    #         {"member": [{"uri":"http://dig.isi.edu/ht/data/webpage/H"}, {"uri": "http://dig.isi.edu/ht/data/webpage/I"}]},
    #         {"member": [{"uri":"http://dig.isi.edu/ht/data/webpage/J"}, {"uri": "http://dig.isi.edu/ht/data/webpage/K"}]},
    #         {"member": [{"uri":"http://dig.isi.edu/ht/data/webpage/L"}, {"uri": "http://dig.isi.edu/ht/data/webpage/M"}]},
    #         {"member": [{"uri":"http://dig.isi.edu/ht/data/webpage/H"}, {"uri": "http://dig.isi.edu/ht/data/webpage/M"}]},
    #         ]
    input = [
        {"member": [{"uri": "A"}, {"uri": "B"}]},
        {"member": [{"uri": "B"}, {"uri": "C"}]},
        {"member": [{"uri": "F"}, {"uri": "G"}]},
        {"member": [{"uri": "H"}, {"uri": "I"}]},
        {"member": [{"uri": "J"}, {"uri": "K"}]},
        {"member": [{"uri": "L"}, {"uri": "M"}]},
        {"member": [{"uri": "N"}, {"uri": "O"}]},

        {"member": [{"uri": "C"}, {"uri": "O"}]},
        {"member": [{"uri": "K"}, {"uri": "L"}]},
        {"member": [{"uri": "H"}, {"uri": "F"}]},
        {"member": [{"uri": "M"}, {"uri": "I"}, {"uri": "N"}]},

        {"member": [{"uri": "D"}, {"uri": "E"}]},
    ]
    rdd = sc.parallelize(input).map(lambda x: ("OPEN", x))
    result_rdd = unionFind.perform(rdd)

    for y in result_rdd.collect():
        print y
        print ""
