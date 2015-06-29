__author__ = 'rajagopal067'


import sys
import fileinput
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import util


#This class has computesimilarity method which computes the similarity between files having 2 different prefixes

class ClusterWithBaseGenerator:
    def __init__(self):
        self.separator = "\t"
        self.scorethreshold = 0.0
        self.outputFilenames = []
        self.numRowsPerFile = 1000000

    def __openFileForWrite(self, filehandle):
        print "Open file: ", filehandle
        sys.stdout.flush()
        return open(filehandle, 'w')

    def __generateNextOutputfile(self, outputFilename):
        file_idx = len(self.outputFilenames)
        out_filename = outputFilename + "." + str(file_idx)
        self.outputFilenames.append(out_filename)
        out = self.__openFileForWrite(out_filename)
        return out

    def run(self, inputFilename, baseFilename, outputFilename, separator, computeSimilarity, scoreThreshold):
        self.separator = separator
        self.scorethreshold = scoreThreshold
        lsh_key = None
        lsh_band = None
        prev_lsh_key = None
        prev_lsh_band = None
        currentClusterInput = []
        currentClusterBase = []

        file_list = [inputFilename, baseFilename]
        tmp_combined_file = inputFilename + "tmp" + ".csv"
        with open(tmp_combined_file, 'w') as file:
           input_lines = fileinput.input(file_list)
           file.writelines(input_lines)

#sort the combined file and then compute similarity between keys in the same band having same lsh keys
        util.sort_csv_file(tmp_combined_file, [0], '\t')
        file = open(tmp_combined_file,'r')

        #file = open(inputFilename, 'r')

        del self.outputFilenames[:]

        out = self.__openFileForWrite(outputFilename)

        for line in file:
            line = line.strip()
            if len(line) > 0:
                lineTokens = line.split(separator)
                lsh_key = lineTokens[0]
                lsh_band = lsh_key[0:3]

                itemId = lineTokens[1]

                if prev_lsh_key is None:
                    prev_lsh_key = lsh_key
                    prev_lsh_band = lsh_band
                    print "Start clustering for Band:", lsh_band
                    sys.stdout.flush()

                if prev_lsh_key != lsh_key:
                     if len(currentClusterInput) > 0:
                        if computeSimilarity:
                            self.__computeSimilarity(currentClusterInput,currentClusterBase,out,lsh_band)

                        else:
                            self.__writeClusters(currentClusterInput,currentClusterBase,out)


                     del currentClusterInput[:]
                     del currentClusterBase[:]

                if prev_lsh_band != lsh_band:
                    print "Start clustering for Band:", lsh_band
                    sys.stdout.flush()

                prev_lsh_key = lsh_key
                prev_lsh_band = lsh_band

                if basePrefix is not None and itemId.startswith(basePrefix):
                    currentClusterBase.append(lineTokens[1:])
                else:
                    currentClusterInput.append(lineTokens[1:])

        if len(currentClusterInput) > 0:
            if computeSimilarity:
                self.__computeSimilarity(currentClusterInput,currentClusterBase,out,lsh_band)

            else:
                self.__writeClusters(currentClusterInput,currentClusterBase,out)

        file.close()
        out.close()
        print "Done computing similarities"
        sys.stdout.flush()


    def __writeClusters(self, keyHashesInput, keyHashesBase, outputFile):
        keyArr = []
        for keyArr1 in keyHashesInput:
            key1 = keyArr1[0]
            keyArr.append(key1)
        for keyArr2 in keyHashesBase:
            key2 = keyArr2[0]
            keyArr.append(key2)

        outputFile.write(util.write_tokens(keyArr, self.separator) + "\n")

    def __computeSimilarity(self, keyHashesInput,keyHashesBase, outputFile, lsh_band):
        #print "Compute Similarity between: ", keys with inputprefix and keys with baseprefix, "
        numLines = 0
        for keyArr1 in keyHashesInput:
            key1 = keyArr1[0]
            minarr1 = keyArr1[1:]
            #print "Start: ", key1
            for keyArr2 in keyHashesBase:
                key2 = keyArr2[0]
                minarr2 = keyArr2[1:]
                if minarr1 != minarr2:
                    score = util.compute_list_similarity(minarr1, minarr2)
                else:
                    score = 1.0
                if score >= self.scorethreshold:
                    if score < 1.0 or lsh_band == "000":
                        outputFile.write(key1 + self.separator + key2 + self.separator + str(score) + "\n")
                        numLines += 1
        return numLines



    def setNumRowsPerFile(self, num):
        self.numRowsPerFile = num


    def remove_duplicates(self, filename):
        util.sort_csv_file(filename, [0, 1], self.separator)

        file_handle = open(filename, 'r')
        tmp_file = open(filename + ".tmp", 'w')
        prev_line = None
        for line in file_handle:
            if prev_line is None or line != prev_line:
                tmp_file.write(line)
            prev_line = line

        os.remove(filename)
        os.rename(filename + ".tmp", filename)


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
computeSimilarity = True
scoreThreshold = 0.0
removeDuplicates = True


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
    global scoreThreshold
    global computeSimilarity

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
        if arg == "--threshold":
            scoreThreshold = float(sys.argv[arg_idx+1])
            continue
        if arg == "--computeSimilarity":
            computeSimilarityStr = (sys.argv[arg_idx+1])
            if computeSimilarityStr == "False":
                computeSimilarity = False
            continue

def die():
    print "Please input the required parameters"
    print "Usage: genClustersWithBase.py --input <input filename> --inputPrefix <input Prefix> --base <base filename> --basePrefix <base prefix> " \
          "--output <output dir> [--separator <sep=\\t>] [--numHashes <numHashes=20>] [--numItemsInBand <numItemsInBand=5>]" \
          " [--minItemsInCluster <minItemsInCluster=2>] [--dataType <default=integer|string>]" \
          " [--threshold <threshold for similarity score to be in one cluster>]"
    exit(1)

parse_args()
if inputFilename is None or outputFilename is None \
        or inputPrefix is None:
    die()

print "Generate Clusters: ", inputFilename,baseFilename,  ", computeSimilarity:", computeSimilarity, ", scoreThreshold:", scoreThreshold, ", removeDuplicates:", removeDuplicates
sys.stdout.flush()

clusterGen = ClusterWithBaseGenerator()

clusterGen.run(inputFilename,baseFilename,outputFilename,separator,computeSimilarity,scoreThreshold)


# this will remove all the duplicates
if computeSimilarity and removeDuplicates:
   clusterGen.remove_duplicates(outputFilename)

