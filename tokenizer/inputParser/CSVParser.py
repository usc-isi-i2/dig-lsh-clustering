#!/usr/bin/env python


class CSVParser:

    def __init__(self, config, options):
        if options is not None:
            self.delimiter = options.separator
        else:
            self.delimiter = "\t"

    def parse(self, x):
        return x.split(self.delimiter)[0], x.split(self.delimiter)[1:]

    def parse_values(self, x):
        return x.split(self.delimiter)
