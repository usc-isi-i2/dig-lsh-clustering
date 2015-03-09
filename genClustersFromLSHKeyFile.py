__author__ = 'dipsy'

import sys
import util

inputFilename = None
outputFilename = None
separator = "\t"


def parse_args():
    global inputFilename
    global outputFilename
    global separator


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


def die():
    print "Please input the required parameters"
    print "Usage: genClustersFromLSHKey.py --input <input filename> --output <output filename> [--separator <sep=\\t>]"
    exit(1)

args = parse_args()
if inputFilename is None or outputFilename is None:
    die()


file = open(inputFilename, 'r')
wFile = open(outputFilename, "w")
prevLSH = None
cluster = []
for line in file:
    parts = line.split("\t")
    lshHash = parts[0]
    #print "Got LSH:" + lshHash + ", prev:" + str(prevLSH)
    key = parts[1]
    if prevLSH is None:
        prevLSH = lshHash

    if prevLSH != lshHash:
        if len(cluster) > 1:
            print "\nWrite Cluster for LSH:" + prevLSH + ":" + str(len(cluster))
            wFile.write(util.write_tokens(cluster, "\t") + "\n")
        del cluster[:]

    cluster.append(key)
    prevLSH = lshHash

if len(cluster) > 1:
    print "\nFinal Write Cluster for LSH:" + prevLSH + ":" + str(len(cluster))
    wFile.write(util.write_tokens(cluster, "\t"))

file.close()
wFile.close()