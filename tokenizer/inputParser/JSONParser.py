#!/usr/bin/env python
import json


class JSONParser:

    def __init__(self, config, options):
        self.column_paths = list()
        for index in config["fieldConfig"]:
            if "path" in config["fieldConfig"][index]:
                self.column_paths.insert(int(index), config["fieldConfig"][index]["path"])

    def parse(self, x):
        json_data = json.loads(x)
        value = self.__extract_columns(json_data)
        key = x["uri"]
        return (key, value)

    def parse_values(self, x):
        json_data = json.loads(x)
        return self.__extract_columns(json_data)

    def __extract_columns(self, row):
        result = []
        for path in self.column_paths:
            #print "Extract path:", self.column_paths
            path_elems = path.split(".")
            start = row
            found = True
            for path_elem in path_elems:
                if path_elem in start:
                    start = start[path_elem]
                    if type(start) == list:
                        start =  ", ".join(start)
                else:
                    found = False
                    break

            if found:
                result.append(start)
            else:
                result.append("")
        #print "Extracted:", result
        return result
