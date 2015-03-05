import util

__author__ = 'dipsy'

import sys
from lsh.lsh import LSH, IntegerLSH
from lsh.lsh import MinHashSignature, IntegerMinHashSignature
import os

inputFilename = None
outputFilename = None
separator = "\t"
numHashes = 20
numItemsInBand = 5
dataType = "integer"

def parse_args():
    global inputFilename
    global outputFilename
    global separator
    global numHashes
    global numItemsInBand
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
        if arg == "--numItemsInBand":
            numItemsInBand = int(sys.argv[arg_idx+1])
            continue
        if arg == "--dataType":
            dataType = sys.argv[arg_idx+1]
            continue

def die():
    print "Please input the required parameters"
    print "Usage: genLSHKeyMinhash.py --input <input filename> --output <output filename> [--separator <sep=\\t>] [--numHashes <numHashes=20>] [--numItemsInBand <numItemsInBand=5>]"
    exit(1)

args = parse_args()
if inputFilename is None or outputFilename is None:
    die()

print "dataType:" + dataType + ", numHashes=" + str(numHashes) + ", numItemsInBand=" + str(numItemsInBand)

signer = None
hasher = None
if dataType == "string":
    signer = MinHashSignature(numHashes)
    hasher = LSH(numHashes, numItemsInBand, None)
else:
    signer = IntegerMinHashSignature(numHashes)
    hasher = IntegerLSH(numHashes, numItemsInBand, None)

# counter = 0
file = open(inputFilename, 'r')
wFile = open(outputFilename, "w")
for line in file:
    idx = line.find("\t")
    # counter = counter + 1
    # if counter > 100:
    #     break
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
                minHash_str = util.write_tokens(sig, separator)
                lshSig = list(hasher.hash(sig))
                for lsh in lshSig:
                    wFile.write(str(lsh) + separator + key + separator + minHash_str + "\n")
file.close()
wFile.close()

util.sort_csv_file(outputFilename, 0, separator)