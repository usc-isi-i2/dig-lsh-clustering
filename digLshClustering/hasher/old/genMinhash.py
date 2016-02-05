__author__ = 'dipsy'

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import util
from hasher.lsh import MinHashSignature, IntegerMinHashSignature

inputFilename = None
outputFilename = None
separator = "\t"
numHashes = 20
dataType = "integer"

def parse_args():
    global inputFilename
    global outputFilename
    global separator
    global numHashes
    global dataType

    for arg_idx, arg in enumerate(sys.argv):
        if arg == "--input":
            inputFilename = sys.argv[arg_idx+1]
            continue
        if arg == "--output":
            outputFilename = sys.argv[arg_idx+1]
            continue
        if arg == "--separator":
            separator = sys.argv[arg_idx+1]
            continue
        if arg == "--numHashes":
            numHashes = int(sys.argv[arg_idx+1])
            continue
        if arg == "--dataType":
            dataType = sys.argv[arg_idx+1]
            continue

def die():
    print "Please input the required parameters"
    print "Usage: genMinhash.py --input <input filename> --output <output filename> [--separator <sep=\\t>] " \
          "[--numHashes <numHashes=20>] [--dataType <default=integer|string>]"
    exit(1)

args = parse_args()
if inputFilename is None or outputFilename is None:
    die()

print "dataType:" + dataType + ", numHashes=" + str(numHashes)

signer = None
if dataType == "string":
    signer = MinHashSignature(numHashes)
else:
    signer = IntegerMinHashSignature(numHashes)

file = open(inputFilename, 'r')
wFile = open(outputFilename, "w")
for line in file:
    idx = line.find("\t")
    if idx != -1:
        key = line[0:idx]
        line_len = len(line)
        data = line[idx + 1:line_len-1].strip() #remove the \n
        if len(data) > 0:
            #print "Data:" + data
            tokens = data.split(separator)
            if dataType == "integer":
                tokens = util.get_int_list(tokens)
            if len(tokens) > 0:
                #print "Adding tokens: " + str(tokens)
                sig = signer.sign(tokens)
                wFile.write(key + separator + util.write_tokens(sig, separator) + "\n")
file.close()
wFile.close()

