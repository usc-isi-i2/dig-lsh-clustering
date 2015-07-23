#!/usr/bin/env python

import sys
import json

if __name__ == "__main__":
    results_file = open(sys.argv[1])
    out_file = open(sys.argv[2], 'w')

    results_json = json.load(results_file)
    hits = results_json["hits"]["hits"]
    for hit in hits:
        fields = hit["fields"]
        data = fields["data"][0]
        data_uri = data["uri"]
        data_addr = data["availableAtOrFrom"]["address"]["name"]
        out_file.write(data_uri + "\t" + data_addr + "\n")

    out_file.close()
