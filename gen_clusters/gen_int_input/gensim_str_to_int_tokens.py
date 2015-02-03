__author__ = 'dipsy'
from gensim import corpora
import sys
import json
import codecs

class Corpus(object):
    def __init__(self, filename, separator):
        self.filename = filename
        self.separator = separator
        self.key_dictionary = corpora.Dictionary()
        self.token_dictionary = corpora.Dictionary()

    def __iter__(self):
        for line in open(self.filename):
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
outputFile = sys.argv[2]
keyHashFile = sys.argv[3]
tokenHashFile = sys.argv[4]

separator = "\t"

if len(sys.argv) > 5:
    separator = sys.argv[5]

outFP = open(outputFile, "w")
corpus = Corpus(inputFile, separator)

for vectors in corpus:
    vector_list = list(vectors)
    outFP.write(write_tokens(vector_list, separator) + "\n")

outFP.close()

keyHashFP = open(keyHashFile, "w")
keyHashFP.write(json.dumps(corpus.get_key_hashmap()))
keyHashFP.close()

tokenHashFP = codecs.open(tokenHashFile, encoding='utf-8', mode='w')
tokenHashFP.write(json.dumps(corpus.get_token_hashmap()))
tokenHashFP.close()

