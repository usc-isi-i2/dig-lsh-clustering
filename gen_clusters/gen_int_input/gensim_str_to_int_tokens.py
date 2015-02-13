__author__ = 'dipsy'
from gensim import corpora
import sys
import json
import codecs
import subprocess

class Corpus(object):
    def __init__(self, filename, filesystem, separator):
        self.filename = filename
        self.filesystem = filesystem
        self.separator = separator
        self.key_dictionary = corpora.Dictionary()
        self.token_dictionary = corpora.Dictionary()

    def __iter__(self):
        stream = None
        if inputFileSystem == "hdfs":
            cat = subprocess.Popen(["hadoop", "fs", "-cat", self.filename], stdout=subprocess.PIPE)
            stream = cat.stdout
        else:
            stream = open(self.filename)
        for line in stream:
            all_tokens = line.lower().split(self.separator)
            key = all_tokens[0]
            tokens = all_tokens[1:]
            self.key_dictionary.doc2bow([key], allow_update=True)
            self.token_dictionary.doc2bow(tokens, allow_update=True)
            #print line
            yield self.get_line_representation(line)

    def get_key_hashmap(self):
        return (self.key_dictionary.token2id)

    def get_token_hashmap(self):
        return self.token_dictionary.token2id

    def save_key_dictionary(self, filename):
        self.key_dictionary.save_as_text(filename)

    def save_token_dictionary(self, filename):
        self.token_dictionary.save_as_text(filename)

    def get_line_representation(self, line):
        all_tokens = line.lower().split(self.separator)
        key = all_tokens[0]
        tokens = all_tokens[1:]
        yield self.get_key_hashmap().get(key)
        for token in tokens:
            yield self.get_token_hashmap().get(token)


def write_tokens(tokens, sep):
    outStr = ""
    usep = ""
    for token in tokens:
        outStr = outStr + usep + str(token)
        usep = sep
    return outStr


inputFile = sys.argv[1]
inputFileSystem = sys.argv[2]
outputFile = sys.argv[3]
keyHashFile = sys.argv[4]
tokenHashFile = sys.argv[5]

separator = "\t"

if len(sys.argv) > 6:
    separator = sys.argv[6]

try:
    outFP = open(outputFile, "w")
    corpus = Corpus(inputFile, inputFileSystem, separator)

    for vectors in corpus:
        vector_list = list(vectors)
        outFP.write(write_tokens(vector_list, separator) + "\n")

    outFP.close()

    corpus.save_key_dictionary(keyHashFile);
    corpus.save_token_dictionary(tokenHashFile);
except Exception as e:
    print "Exception({0}): {1}".format(e.errno, e.strerror)


