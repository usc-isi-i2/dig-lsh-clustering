#!/usr/bin/env python
import json


class JSONParser:

    def __init__(self, config, options):
        self.column_paths = None
        #TODO: extract column_paths from the config

    def parse(self, x):
        json_data = json.loads(x)
        #TODO

    def parse_values(self, x):
        json_data = json.loads(x)
        #TODO

    def __extract_columns(self, row):
        result = []
        for path in self.column_paths:
            path_elems = path.split(".")
            start = row
            found = True
            for path_elem in path_elems:
                if path_elem in start:
                    start = start["path_elem"]
                else:
                    found = False
                    break

            if found:
                result.append(start)
            else:
                result.append("")
        return result
