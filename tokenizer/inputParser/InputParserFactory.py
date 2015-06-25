#!/usr/bin/env python

from CSVParser import CSVParser
from JSONParser import JSONParser


class ParserFactory:
    def __init__(self):
        pass

    @staticmethod
    def get_parser(name, config, options):
        print "Get parser:", name
        if name == "csv":
            return CSVParser(config, options)
        elif name == "json":
            return JSONParser(config, options)
        return None