import util

__author__ = 'dipsy'


import sys
import fileinput
import os

inputFilename = None
baseFilename = None
inputPrefix = None
basePrefix = None
outputFilename = None
separator = "\t"
numHashes = 20
numItemsInBand = 5
minItemsInCluster = 2
dataType = "integer"


def parse_args():
    global inputFilename
    global inputPrefix
    global outputFilename
    global separator
    global numHashes
    global numItemsInBand
    global baseFilename
    global basePrefix
    global minItemsInCluster
    global dataType

    for arg_idx, arg in enumerate(sys.argv):
        if arg == "--input":
            inputFilename = sys.argv[arg_idx+1]
            continue
        if arg == "--inputPrefix":
            inputPrefix = sys.argv[arg_idx+1]
            continue
        if arg == "--base":
            baseFilename = sys.argv[arg_idx+1]
            continue
        if arg == "--basePrefix":
            basePrefix = sys.argv[arg_idx+1]
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
    print "Usage: genClusters.py --input <input filename> --inputPrefix <input Prefix> --base <base filename> --basePrefix <base prefix> " \
          "--output <output dir> [--separator <sep=\\t>] [--numHashes <numHashes=20>] [--numItemsInBand <numItemsInBand=5>]" \
          " [--minItemsInCluster <minItemsInCluster=2>] [--dataType <default=integer|string>]"
    exit(1)

parse_args()
if inputFilename is None or outputFilename is None \
        or baseFilename is None or basePrefix is None\
        or inputPrefix is None:
    die()

print "dataType:" + dataType + ", numHashes=" + str(numHashes) + ", numItemsInBand=" + str(numItemsInBand)\
      + ", minItemsInCluster=" + str(minItemsInCluster)


file_list = [inputFilename, baseFilename]
tmp_combined_file = inputFilename + ".tmp"
with open(tmp_combined_file, 'w') as file:
    input_lines = fileinput.input(file_list)
    file.writelines(input_lines)

util.sort_csv_file(tmp_combined_file, 0, '\t')

rFile = open(tmp_combined_file, 'r')
wFile = open(outputFilename, 'w')
currentLshKey = None
currentClusterInput = []
currentClusterBase = []
for line in rFile:
    lineParts = line.strip().split("\t")
    lshKey = lineParts[0]
    itemId = lineParts[1]
    if currentLshKey is None:
        currentLshKey = lshKey

    if currentLshKey != lshKey:
        if len(currentClusterInput) > 0:
            for inputItem in currentClusterInput:
                wFile.write(inputItem + separator + util.write_tokens(currentClusterBase, separator) + "\n")
        del currentClusterInput[:]
        del currentClusterBase[:]
    currentLshKey = lshKey
    if itemId.startswith(basePrefix):
        currentClusterBase.append(itemId)
    else:
        currentClusterInput.append(itemId)
rFile.close()
wFile.close()

os.unlink(tmp_combined_file)


print "Done generating clusters"

