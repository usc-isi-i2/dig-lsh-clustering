#!/usr/bin/env python

import sys
import json


def find_truth(line, hits):
    line_parts = line.split("\t")
    weapons_uri = line_parts[0].strip()
    geonames_uri = line_parts[4].strip()
    # print weapons_uri + " matches " + geonames_uri
    for hit in hits:
        fields = hit["fields"]
        data = fields["data"][0]
        if data["uri"] == weapons_uri:
            # print "Found uri, now check matches"
            if "geonames_addresses" in fields["geonames"][0]:
                matches = fields["geonames"][0]["geonames_addresses"]
                # print "LSH has ", len(matches), " candidates"
                idx = match_index(geonames_uri, matches)
                if idx != -1:
                    # print "\tFound match at index:", idx
                    return True
    print "\tNo match found: ",  weapons_uri + " matches " + geonames_uri
    return False

def match_index(geonames_uri, matches):
    idx = -1
    for match in matches:
        idx += 1
        # "Check:", geonames_uri + " to ", match["uri"]
        if match["uri"] == geonames_uri:
            return idx
    return -1

# Query run:
# {
#   "query": {
#     "ids": {
#         "values": [
#             "http://dig.isi.edu/weapons/data/page/000A94AA128DAE1B5F8CA5CD81312CE6F950EFDB/1433027602000/processed",
#             ...
#         ]
#     }
#   },
#    "filter" : {
#         "exists" : { "field" : "availableAtOrFrom" }
#
#     },
#     "partial_fields" : {
#
#         "geonames": {
#             "include": ["geonames_addresses.uri", "geonames_addresses.score", "geonames_addresses.fallsWithinState1stDiv.hasName.label",
#                     "geonames_addresses.hasPreferredName.label"]
#         },
#         "data" : {
#             "include" : ["uri","availableAtOrFrom.address.name"]
#         }
#     },
#      "from" : 0, "size" : 1000
# }

if __name__ == "__main__":
    truth_file = open(sys.argv[1])
    results_file = open(sys.argv[2])
    results_json = json.load(results_file)
    hits = results_json["hits"]["hits"]

    numFound = 0
    num = 0
    for line in truth_file:
        num += 1
        if num == 1:
            continue
        #print "Got truth:", line

        found = find_truth(line, hits)
        if found is True:
            numFound += 1

    print "Num:", num
    print "Num results found:", numFound