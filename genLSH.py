import util

__author__ = 'dipsy'

import sys
from lsh.lsh import LSH, IntegerLSH
import os

inputFilename = None
outputDir = None
separator = "\t"
numHashes = 20
numItemsInBand = 5
dataType = "integer"

def parse_args():
    global inputFilename
    global outputDir
    global separator
    global numHashes
    global numItemsInBand
    global dataType

    for arg_idx, arg in enumerate(sys.argv):
        if arg == "--input":
            inputFilename = sys.argv[arg_idx+1]
            continue
        if arg == "--output":
            outputDir = sys.argv[arg_idx+1]
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
    print "Usage: genLSH.py --input <input filename> --output <output dir> [--separator <sep=\\t>] [--numHashes <numHashes=20>] [--numItemsInBand <numItemsInBand=5>]"
    exit(1)

args = parse_args()
if inputFilename is None or outputDir is None:
    die()

hasher = None
if dataType == "integer":
    hasher = IntegerLSH(numHashes, numItemsInBand, None)
else:
    hasher = LSH(numHashes, numItemsInBand, None)

file = open(inputFilename, 'r')

numOutputFiles = numHashes/numItemsInBand
wFiles = []
if not os.path.exists(outputDir):
    os.mkdir(outputDir)
for i in range(0, numOutputFiles):
    oFile = open(outputDir + "/lsh-" + str(i) + ".tsv", "w")
    wFiles.append(oFile)

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
                sig = list(hasher.hash(tokens))
                for i in range(0, numOutputFiles):
                    wFiles[i].write(sig[i] + separator + key + "\n")
file.close()
for i in range(0, numOutputFiles):
    wFiles[i].close()

for i in range(0, numOutputFiles):
    filename = outputDir + "/lsh-" + str(i) + ".tsv"
    util.sort_csv_file(filename, 0, separator)
