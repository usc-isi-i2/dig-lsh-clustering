#!/usr/bin/env python
import json


class JSONParser:

    def __init__(self, config, options):
        self.column_paths = list()
        for index in config["fieldConfig"]:
            if "path" in config["fieldConfig"][index]:
                self.column_paths.insert(int(index), config["fieldConfig"][index]["path"])

    def parse(self, x):
        return self.parse_with_paths(x, "uri", self.column_paths)

    def parse_with_key(self, x, key_name):
        return self.parse_values_with_paths(x, key_name, self.column_paths)

    def parse_with_paths(self, x, key_name, paths):
        json_data = json.loads(x)
        value = self.__extract_columns(json_data, paths)
        key = x[key_name]
        return key, value

    def parse_values(self, x):
        return self.parse_values_with_paths(x, self.column_paths)

    def parse_values_with_paths(self, x, paths):
        json_data = json.loads(x)
        return self.__extract_columns(json_data, paths)

    def __extract_columns(self, row, paths):
        result = []
        for path in paths:
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
