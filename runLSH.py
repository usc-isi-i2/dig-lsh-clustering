__author__ = 'dipsy'

from lsh.lsh import Cluster, IntegerCluster
import sys
import json

inputFilename = None
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
    global minItemsInCluster
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
        if arg == "--minItemsInCluster":
            minItemsInCluster = int(sys.argv[arg_idx+1])
            continue
        if arg == "--dataType":
            dataType = sys.argv[arg_idx+1]
            continue

def die():
    print "Please input the required parameters"
    print "Usage: runLSH.py --input <input filename> --output <output filename> [--separator <sep=\\t>] [--numHashes <numHashes=20>] [--numItemsInBand <numItemsInBand=5>] [--minItemsInCluster <minItemsInCluster=2>] [--dataType <default=integer|string>]"
    exit(1)

parse_args()
if inputFilename is None:
    die()
if outputFilename is None:
    die()

print "Using dataType:" + dataType +", numHashes:" + str(numHashes) + ", numItemsInBand:" + str(numItemsInBand) + ", minItemsInCluster:" + str(minItemsInCluster)
if dataType == "string":
    cluster = Cluster(numHashes, numItemsInBand)
else:
    cluster = IntegerCluster(numHashes, numItemsInBand)
file = open(inputFilename, 'r')
for line in file:
    idx = line.find("\t")
    if idx != -1:
        key = line[0:idx]
        line_len = len(line)
        data = line[idx + 1:line_len-1] #remove the \n

        tokens = data.split(separator)
        if len(tokens) > 0:
            # print "Adding tokens: " + str(tokens)
            cluster.add_set(tokens, key)

file.close()

result = list(cluster.get_clusters(minItemsInCluster))
wFile = open(outputFilename, "w")
wFile.write("{\n")
wFile.write("\"numClusters\":" + str(len(result)) + ",\n")
wFile.write("\"lshSettings\":\n")
wFile.write("\t{\n")
wFile.write("\t\"numHashes\":" + str(numHashes) + ",\n")
wFile.write("\t\"numItemsInBand\":" + str(numItemsInBand) + "\n")
wFile.write("\t},\n")
wFile.write("\"clusters\":\n")

wFile.write("\t[\n")
sep = ""
for cluster_item in result:
    wFile.write(sep)
    wFile.write("\t{\"cluster\": \n")
    str = json.dumps(cluster_item, indent=16)
    str = str[1:len(str)-1]
    wFile.write("\t\t[" + str + "\t\t]\n")
    wFile.write("\t}")
    sep = ",\n"
wFile.write("\t]\n")
wFile.write("}\n")
