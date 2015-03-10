import util

__author__ = 'dipsy'


import sys
from lsh.lsh import LSH, IntegerLSH
from util import Searcher

inputFilename = None
LSHdir = None
outputFilename = None
separator = "\t"
numHashes = 20
numItemsInBand = 5
minItemsInCluster = 2
dataType = "integer"

def parse_args():
    global inputFilename
    global outputFilename
    global separator
    global numHashes
    global numItemsInBand
    global LSHdir
    global minItemsInCluster
    global dataType

    for arg_idx, arg in enumerate(sys.argv):
        if arg == "--input":
            inputFilename = sys.argv[arg_idx+1]
            continue
        if arg == "--LSHdir":
            LSHdir = sys.argv[arg_idx+1]
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
        if arg == "--minItemsInCluster":
            minItemsInCluster = int(sys.argv[arg_idx+1])
            continue
        if arg == "--dataType":
            dataType = sys.argv[arg_idx+1]
            continue

def die():
    print "Please input the required parameters"
    print "Usage: genClusters.py --input <input filename> --output <output dir> [--separator <sep=\\t>] [--numHashes <numHashes=20>] [--numItemsInBand <numItemsInBand=5>] [--minItemsInCluster <minItemsInCluster=2>] [--dataType <default=integer|string>]"
    exit(1)

def get_lsh_key_from_line(line):
    return line.split("\t")[0]

args = parse_args()
if inputFilename is None or outputFilename is None or LSHdir is None:
    die()

print "dataType:" + dataType + ", numHashes=" + str(numHashes) + ", numItemsInBand=" + str(numItemsInBand)\
      + ", minItemsInCluster=" + str(minItemsInCluster)

file = open(inputFilename, 'r')
hasher = None
if dataType == "integer":
    hasher = IntegerLSH(numHashes, numItemsInBand, None)
else:
    hasher = LSH(numHashes, numItemsInBand, None)

oFile = open(outputFilename, "w")

numOutputFiles = numHashes/numItemsInBand
lshSearchers = []
for i in range(0, numOutputFiles):
    lFile = LSHdir + "/lsh-" + str(i) + ".tsv"
    searcher = util.Searcher(lFile)
    lshSearchers.append(searcher)

for line in file:
    idx = line.find("\t")
    if idx != -1:
        key = line[0:idx]
        line_len = len(line)
        data = line[idx + 1:line_len-1].strip() #remove the \n
        if len(data) > 0:
            tokens = data.split(separator)
            if len(tokens) > 0:
                sig = list(hasher.hash(tokens))
                matches = []
                for i in range(0, len(sig)):
                    lshKey = sig[i]
                    #print "Find " + lshKey + " in file: "
                    matchLines = lshSearchers[i].find(lshKey, key=get_lsh_key_from_line)

                    if matchLines is not None and len(matchLines) > 0:
                        for matchLine in matchLines:
                            matchLine = matchLine.strip()
                            match = matchLine.split(separator)[1]
                            matches.append(match)

                if len(matches) > minItemsInCluster:
                    #print "Write " + lshKey + ":" +  str(len(matches))
                    oFile.write(key + separator + util.write_tokens(matches, separator) + "\n")

file.close()
oFile.close()

print "Done generating clusters"

