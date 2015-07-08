#!/usr/bin/env python

from pyspark import SparkContext
from optparse import OptionParser
from inputParser.InputParserFactory import ParserFactory
from RowTokenizer import RowTokenizer
import json

class Tokenizer:
    def __init__(self, config_filename, p_options):
        self.options = p_options
        config_file = open(config_filename)
        self.config = json.load(config_file)
        pass

    def tokenize_seq_file(self, spark_context, filename, data_type):
        raw_data = spark_context.sequenceFile(filename)
        input_parser = ParserFactory.get_parser(data_type, self.config, self.options)
        if input_parser:
            data = raw_data.mapValues(lambda x: input_parser.parse_values(x))
            return self.tokenize_rdd(data)

    def tokenize_text_file(self, spark_context, filename, data_type):
        raw_data = spark_context.textFile(filename)
        input_parser = ParserFactory.get_parser(data_type, self.config, self.options)
        if input_parser:
            data = raw_data.map(lambda x: input_parser.parse(x))
            return self.tokenize_rdd(data)

    def tokenize_rdd(self, data):
        return data.flatMapValues(lambda row: self.__get_tokens(row))

    def __get_tokens(self, row):
        row_tokenizer = RowTokenizer(row, self.config)

        line = row_tokenizer.next()
        while line:
            #print "RETURN", line[0:100]
            yield line
            line = row_tokenizer.next()


if __name__ == "__main__":
    """
        Usage: tokenizer.py [input] [config] [output]
    """
    sc = SparkContext(appName="LSH-TOKENIZER")

    usage = "usage: %prog [options] input config output"
    parser = OptionParser()
    parser.add_option("-r", "--separator", dest="separator", type="string",
                      help="field separator", default="\t")
    parser.add_option("-d", "--type", dest="data_type", type="string",
                      help="input data type: csv/json", default="csv")
    parser.add_option("-i", "--inputformat", dest="inputformat", type="string",
                      help="input file format: text/sequence", default="text")

    (c_options, args) = parser.parse_args()
    print "Got options:", c_options
    inputFilename = args[0]
    configFilename = args[1]
    outputFilename = args[2]

    tokenizer = Tokenizer(configFilename, c_options)
    if c_options.inputformat == "text":
        rdd = tokenizer.tokenize_text_file(sc, inputFilename, c_options.data_type)
    else:
        rdd = tokenizer.tokenize_seq_file(sc, inputFilename, c_options.data_type)

    #rdd.saveAsTextFile(outputFilename)
    rdd.mapValues(lambda x: json.dumps(x)).saveAsSequenceFile(outputFilename)
