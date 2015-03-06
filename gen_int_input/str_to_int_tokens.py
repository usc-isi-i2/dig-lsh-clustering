__author__ = 'dipsy'
from gensim import corpora
import sys
import subprocess


class Corpus(object):
    def __init__(self, filename, filesystem, separator, convertKeys):
        self.filename = filename
        self.filesystem = filesystem
        self.separator = separator
        if convertKeys:
            self.key_dictionary = corpora.Dictionary()
        else:
            self.key_dictionary = None
        self.token_dictionary = corpora.Dictionary()

    def __iter__(self):
        stream = None
        if inputFileSystem == "hdfs":
            cat = subprocess.Popen(["hadoop", "fs", "-cat", self.filename], stdout=subprocess.PIPE)
            stream = cat.stdout
        else:
            stream = open(self.filename)
        for line in stream:
            all_tokens = line.lower().strip().split(self.separator)
            key = all_tokens[0].strip()
            tokens = all_tokens[1:]
            if self.key_dictionary:
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

    def load_key_dictionary(self, filename):
        self.key_dictionary.load_as_text(filename)

    def save_token_dictionary(self, filename):
        self.token_dictionary.save_as_text(filename)

    def load_token_dictionary(self, filename):
        self.token_dictionary.load_from_text(filename)

    def get_line_representation(self, line):
        all_tokens = line.lower().strip().split(self.separator)
        key = all_tokens[0]
        tokens = all_tokens[1:]
        if self.key_dictionary:
            yield self.get_key_hashmap().get(key)
        else:
            yield key
        for token in tokens:
            yield self.get_token_hashmap().get(token)


inputFile = None
inputFileSystem = "file"
outputFile = None
keyHashFile = None
tokenHashFile = None
separator = "\t"
inputTokensHashFile = None

def parse_args():
    global inputFile
    global inputFileSystem
    global outputFile
    global keyHashFile
    global tokenHashFile
    global separator
    global inputTokensHashFile

    for arg_idx, arg in enumerate(sys.argv):
        if arg == "--input":
            inputFile = sys.argv[arg_idx+1]
            continue
        if arg == "--output":
            outputFile = sys.argv[arg_idx+1]
            continue
        if arg == "--inputFS":
            inputFileSystem = sys.argv[arg_idx+1]
            continue
        if arg == "--keysFile":
            keyHashFile = (sys.argv[arg_idx+1])
            continue
        if arg == "--tokensFile":
            tokenHashFile = (sys.argv[arg_idx+1])
            continue
        if arg == "--separator":
            separator = sys.argv[arg_idx+1]
            continue
        if arg == "--inputTokensFile":
            inputTokensHashFile = sys.argv[arg_idx+1]
            continue

def die():
    print "Please input the required parameters"
    print "Usage: str_to_int_tokens.py --input <input filename> [--inputFS <hdfs|default=file>] " \
          "--output <output filename> --keysFile <keys map filename> --tokensFile <tokens map filename>" \
          "--separator <separator-default=tab> [--inputTokensFile <Tokens Hashfile to use as input>]"
    exit(1)

def write_tokens(tokens, sep):
    outStr = ""
    usep = ""
    for token in tokens:
        outStr = outStr + usep + str(token)
        usep = sep
    return outStr

args = parse_args()
if inputFile is None or outputFile is None or tokenHashFile is None:
    die()

try:
    outFP = open(outputFile, "w")
    corpus = Corpus(inputFile, inputFileSystem, separator, keyHashFile is not None)
    if inputTokensHashFile is not None:
        corpus.load_token_dictionary(inputTokensHashFile)

    for vectors in corpus:
        vector_list = list(vectors)
        outFP.write(write_tokens(vector_list, separator) + "\n")

    outFP.close()

    if keyHashFile is not None:
        corpus.save_key_dictionary(keyHashFile)
    corpus.save_token_dictionary(tokenHashFile)
    print "Done conversion"
except Exception as e:
    print "Exception:" + e.message
