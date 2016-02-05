__author__ = 'dipsy'
from gensim import corpora
import sys
import subprocess
import os

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
        print "Created blank token dictionary"

    def __iter__(self):
        print "Start the conversion"
        filenames = self.filename.split(",")
        for filename in filenames:
            print "File: ", filename
            stream_arr = []
            if inputFileSystem == "hdfs":
                cat = subprocess.Popen(["hadoop", "fs", "-cat", filename], stdout=subprocess.PIPE)
                stream = cat.stdout
                stream_arr.append(stream)
            else:
                if os.path.isdir(filename):
                    print "This is a directory, get all files"
                    filename_arr = os.listdir(filename)
                    for inner_filename in filename_arr:
                        full_name = os.path.join(filename, inner_filename)
                        stream = open(full_name)
                        stream_arr.append(stream)
                else:
                    print "INput is a file"
                    stream = open(filename)
                    stream_arr.append(stream)

            for stream in stream_arr:
                print "Got stream:", stream
                for line in stream:
                    line = line.decode("utf-8")
                    idx = line.find(self.separator)
                    if idx != -1:
                        key = line[0:idx].strip()
                        tokens = line[idx+1:].strip().lower().split(self.separator)
                        if self.key_dictionary:
                            self.key_dictionary.doc2bow([key], allow_update=True)
                        self.token_dictionary.doc2bow(tokens, allow_update=True)
                        #print line
                        yield self.get_line_representation(key, tokens)

    def get_key_hashmap(self):
        return (self.key_dictionary.token2id)

    def get_token_hashmap(self):
        return self.token_dictionary.token2id

    def save_key_dictionary(self, filename):
        self.key_dictionary.save(filename)
        self.key_dictionary.save_as_text(filename + ".txt")

    def load_key_dictionary(self, filename):
        self.key_dictionary.load(filename)

    def save_token_dictionary(self, filename):
        self.token_dictionary.save(filename)
        self.token_dictionary.save_as_text(filename + ".txt")
        print self.token_dictionary

    def load_token_dictionary(self, filename):
        print "Load the token dictionary from file: " + filename
        self.token_dictionary = corpora.Dictionary.load(filename)
        print self.token_dictionary

    def get_line_representation(self, key, tokens):
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
        if arg == "--outputKeysFile":
            keyHashFile = (sys.argv[arg_idx+1])
            continue
        if arg == "--outputTokensFile":
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
          "--output <output filename> --outputKeysFile <keys map filename> --outputTokensFile <tokens map filename>" \
          "--separator <separator-default=tab> [--inputTokensFile <Tokens Hashfile to use as input>]"
    exit(1)

def write_tokens(tokens, sep):
    outStr = ""
    usep = ""
    for token in tokens:
        outStr = outStr.encode("ascii",'ignore') + usep.encode("ascii","ignore") + str(token if isinstance(token,int) else token.encode("ascii","ignore"))
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
