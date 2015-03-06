import util

__author__ = 'dipsy'


import sys
from lsh.lsh import LSH


inputFilename = None
LSHdir = None
outputFilename = None
separator = "\t"
numHashes = 20
numItemsInBand = 5
minItemsInCluster = 2

def parse_args():
    global inputFilename
    global outputFilename
    global separator
    global numHashes
    global numItemsInBand
    global LSHdir
    global minItemsInCluster

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

def die():
    print "Please input the required parameters"
    print "Usage: genClusters.py --input <input filename> --output <output dir> [--separator <sep=\\t>] [--numHashes <numHashes=20>] [--numItemsInBand <numItemsInBand=5>] [--minItemsInCluster <minItemsInCluster=2>] "
    exit(1)

def get_lsh_key_from_line(line):
    return line.split("\t")[0]

args = parse_args()
if inputFilename is None or outputFilename is None or LSHdir is None:
    die()


file = open(inputFilename, 'r')
hasher = LSH(numHashes, numItemsInBand, None)
oFile = open(outputFilename, "w")

numOutputFiles = numHashes/numItemsInBand
lshFiles = []
for i in range(0, numOutputFiles):
    lFile = LSHdir + "/lsh-" + str(i) + ".tsv"
    lshFiles.append(lFile)

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
                matches = []
                for i in range(0, len(sig)):
                    lshKey = sig[i]
                    matchLines = util.binary_search_in_file(lshFiles[i], lshKey, key=get_lsh_key_from_line)
                    if matchLines is not None and len(matchLines) > 0:
                        for matchLine in matchLines:
                            matchLine = matchLine.strip()
                            #print matchLine
                            match = matchLine.split(separator)[1]
                            matches.append(match)

                if len(matches) > minItemsInCluster:
                    oFile.write(key + separator + util.write_tokens(matches, separator) + "\n")
file.close()
oFile.close()

print "Done generating clusters"

