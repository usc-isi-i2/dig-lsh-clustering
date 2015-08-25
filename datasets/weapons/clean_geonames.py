#!/usr/bin/env python

def clean_geonames(item):
    if "geonames_address" in item:
        addresses = item["geonames_address"]
        result = []
        for addr in addresses:
            geo = {}
            geo["lat"] = addr["geo"]["lat"]
            geo["lon"] = addr["geo"]["lon"]
            addr["geo"] = geo
            if "hasAlternateName" in addr:
                del addr["hasAlternateName"]
            result.append(addr)
        item["geonames_address"] = result
    return item

if __name__ == "__main__":
    from pyspark import SparkContext
    import json
    import sys

    sc = SparkContext(appName="LSH")
    inputFilename = sys.argv[1]
    outputFilename = sys.argv[2]

    rdd = sc.sequenceFile(inputFilename)
    json_rdd = rdd.mapValues(lambda x: json.loads(x))
    revised_rdd = json_rdd.mapValues(lambda x: clean_geonames(x))
    revised_rdd.mapValues(lambda x: json.dumps(x)).saveAsSequenceFile(outputFilename)