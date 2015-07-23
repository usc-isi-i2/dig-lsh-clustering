#!/usr/bin/env python

import sys
import json

def find_truth(line, hits):
    line_parts = line.split("\t")
    weapons_uri = line_parts[0].strip()
    geonames_uri = line_parts[4].strip()

    for hit_json in hits:
        if weapons_uri == hit_json["uri"]:
            start = 1
            for match in hit_json["geonames_addresses"]:
                if match["uri"] == geonames_uri:
                    print weapons_uri, ":", match["uri"], ": Match found at index:", start, ", score:", match["score"]
                    return True
                start += 1
            break

    print "No match found: ",  weapons_uri + " matches " + geonames_uri
    return False

def match_index(geonames_uri, matches):
    idx = -1
    for match in matches:
        idx += 1
        # "Check:", geonames_uri + " to ", match["uri"]
        if match["uri"] == geonames_uri:
            return idx
    return -1

if __name__ == "__main__":
    truth_file = open(sys.argv[1])
    results_file = open(sys.argv[2])

    numFound = 0
    num = 0

    hits = []
    for hit in results_file:
        hit = hit.strip().replace("'", "\"")
        hit_json = json.loads(hit)
        hits.append(hit_json)

    prev_weapons_uri = ""

    for line in truth_file:

        num += 1
        if num == 1:
            continue
        #print "Got truth:", line

        line_parts = line.split("\t")
        weapons_uri = line_parts[0].strip()
        if weapons_uri != prev_weapons_uri and prev_weapons_uri != "":
            print "\n\n"

        prev_weapons_uri = weapons_uri
        found = find_truth(line, hits)
        if found is True:
            numFound += 1

    print "Num:", num
    print "Num results found:", numFound