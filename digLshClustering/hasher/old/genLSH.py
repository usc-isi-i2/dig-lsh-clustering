__author__ = 'dipsy'

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import util
from hasher.lsh import LSH, IntegerLSH, MinHashSignature, IntegerMinHashSignature

inputFilename = None
inputType = "tokens"
outputFilename = None
separator = "\t"
numHashes = 20
numItemsInBand = 5
dataType = "integer"
keyPrefix = ""
sortOutput = False
outputMinhash = False

def parse_args():
    global inputFilename
    global outputFilename
    global separator
    global numHashes
    global numItemsInBand
    global dataType
    global keyPrefix
    global inputType
    global sortOutput
    global outputMinhash

    for arg_idx, arg in enumerate(sys.argv):
        if arg == "--input":
            inputFilename = sys.argv[arg_idx+1]
            continue
        if arg == "--inputType":
            inputType = sys.argv[arg_idx+1]
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

        if arg == "--keyPrefix":
            keyPrefix = sys.argv[arg_idx+1]
            continue
        if arg == "--sortOutput":
            sortOutputStr = (sys.argv[arg_idx+1])
            if sortOutputStr == "True":
                sortOutput = True
            continue
        if arg == "--outputMinhash":
            outputMinhashStr = (sys.argv[arg_idx+1])
            if outputMinhashStr == "True":
                outputMinhash = True
            continue


def die():
    print "Please input the required parameters"
    print "Usage: genLSH.py --input <input filename> [--inputType <tokens|minhash>] --output <output filename> [--separator <sep=\\t>] " \
          "[--numHashes <numHashes=20>] [--numItemsInBand <numItemsInBand=5>] [--dataType <default=integer|string>]" \
          "[--keyPrefix <prefix for key] [--sortOutput <True|False=default] [--outputMinhash <True|False=default>"
    exit(1)

args = parse_args()
if inputFilename is None or outputFilename is None:
    die()

print "Generate LSh Keys: numHashes:", numHashes, ", numItemsInBand:", numItemsInBand, ", dataType:", dataType, ", sortOutput:", sortOutput, ", inputType:", inputType
hasher = None
signer = None
if dataType == "integer":
    hasher = IntegerLSH(numHashes, numItemsInBand, None)
else:
    hasher = LSH(numHashes, numItemsInBand, None)
if inputType == "tokens":
    if dataType == "integer":
        signer = IntegerMinHashSignature(numHashes)
    else:
        signer = MinHashSignature(numHashes)

file = open(inputFilename, 'r')
wFile = open(outputFilename, 'w')
numBands = numHashes/numItemsInBand

for line in file:
    idx = line.find("\t")
    if idx != -1:
        key = line[0:idx]
        line_len = len(line)
        data = line[idx + 1:line_len-1].strip() #remove the \n
        if len(data) > 0:
            #print "Data:" + data
            tokens = data.split(separator)
            if len(tokens) > 0:
                #print "Adding tokens: " + str(tokens)
                minHashSig = None
                if inputType == "tokens":
                    if dataType == "integer":
                        tokens = util.get_int_list(tokens)
                    #print "Sign", tokens
                    if len(tokens) > 0:
                        minHashSig = signer.sign(tokens)
                else:
                    minHashSig = tokens

                #print "Key: ", key + ", minHash", minHashSig
                if minHashSig is not None:
                    lshSig = list(hasher.hash(minHashSig))
                    minOut = ""
                    if outputMinhash:
                        minOut = separator + util.write_tokens(minHashSig, separator)

                    for i in range(0, numBands):
                        wFile.write(str(i).zfill(3) + ":" + lshSig[i] + separator + keyPrefix + key + minOut + "\n")

file.close()
wFile.close()

if sortOutput:
    print "Sorting output on LSH Keys.."
    util.sort_csv_file(outputFilename, [0], separator)
