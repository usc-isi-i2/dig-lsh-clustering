__author__ = 'dipsy'
import sys
import util
import os
from hdfs.hfile import Hfile

class ClusterGenerator(object):

    def __init__(self):
        self.separator = "\t"
        self.scorethreshold = 0.0
        self.outputFilenames = []
        self.numRowsPerFile = 1000000


    def __openFileForWrite(self, filehandle):
        if filehandle.startswith("hdfs://"):
            path = filehandle[7:]
            idx = path.find(":")
            host = path[0:idx]
            path = path[idx+1:]
            idx = path.find("/")
            port = int(path[0:idx])
            filename = path[idx:]
            print "Open HDFS file: host:", host + ",port:", port, ", filename:", filename
            sys.stdout.flush()
            return Hfile(host, port, filename, mode='w')
        else:
            print "Open file: ", filehandle
            sys.stdout.flush()
            return open(filehandle, 'w')

    def __generateNextOutputfile(self, outputFilename):
        file_idx = len(self.outputFilenames)
        out_filename = outputFilename + "." + str(file_idx)
        self.outputFilenames.append(out_filename)
        out = self.__openFileForWrite(out_filename)
        return out


    def run(self, inputFilename, outputFilename, separator, computeSimilarity, scoreThreshold):
        self.separator = separator
        self.scorethreshold = scoreThreshold

        itemKey_minhashes = []
        lsh_key = None
        lsh_band = None
        prev_lsh_key = None
        prev_lsh_band = None

        file = open(inputFilename, 'r')
        del self.outputFilenames[:]

        out = self.__generateNextOutputfile(outputFilename)
        num_lines = 0
        for line in file:
            line = line.strip()
            if len(line) > 0:
                lineTokens = line.split(separator)
                lsh_key = lineTokens[0]
                lsh_band = lsh_key[0:3]

                itemKey_minhash = lineTokens[1:]

                if prev_lsh_key is None:
                    prev_lsh_key = lsh_key
                    prev_lsh_band = lsh_band
                    print "Start clustering for Band:", lsh_band
                    sys.stdout.flush()

                if prev_lsh_key != lsh_key:
                    if len(itemKey_minhashes) > 1:
                        if computeSimilarity:
                            num_lines_written = self.__computeSimilarity(itemKey_minhashes, out, lsh_band)
                            num_lines += num_lines_written
                        else:
                            self.__writeClusters(itemKey_minhashes, out)
                            num_lines += 1
                    del itemKey_minhashes[:]

                if prev_lsh_band != lsh_band:
                    print "Start clustering for Band:", lsh_band
                    sys.stdout.flush()

                prev_lsh_key = lsh_key
                prev_lsh_band = lsh_band

                itemKey_minhashes.append(itemKey_minhash)
                if num_lines > self.numRowsPerFile:
                    num_lines = 0
                    out.close()
                    out = self.__generateNextOutputfile(outputFilename)

        file.close()
        out.close()
        print "Done computing similarities"
        sys.stdout.flush()

    def __writeClusters(self, keyHashesArray, outputFile):
        keyArr = []
        for keyArr1 in keyHashesArray:
            key1 = keyArr1[0]
            keyArr.append(key1)

        outputFile.write(util.write_tokens(keyArr, self.separator) + "\n")


    def __computeSimilarity(self, keyHashesArray, outputFile, lsh_band):
        #print "Compute Similarity between: ", len(keyHashesArray), " items"
        numLines = 0
        for keyArr1 in keyHashesArray:
            key1 = keyArr1[0]
            minarr1 = keyArr1[1:]
            #print "Start: ", key1
            for keyArr2 in keyHashesArray:
                key2 = keyArr2[0]
                if key1 < key2:
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


    def getOutputfilenames(self):
        return self.outputFilenames


    def setNumRowsPerFile(self, num):
        self.numRowsPerFile = num


    def removeDuplicates(self, filename):
        util.sort_csv_file(filename, [0,1], self.separator)

        file = open(filename, 'r')
        tmpFile = open(filename + ".tmp", 'w')
        prev_line = None
        for line in file:
            if prev_line is None or line != prev_line:
                tmpFile.write(line)
            prev_line = line

        os.remove(filename)
        os.rename(filename + ".tmp", filename)

inputFilename = None
outputFilename = None
separator = "\t"
dataType = "integer"
computeSimilarity = True
scoreThreshold = 0.0
removeDuplicates = True

def parse_args():
    global inputFilename
    global outputFilename
    global separator
    global scoreThreshold
    global computeSimilarity
    global removeDuplicates

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
        if arg == "--threshold":
            scoreThreshold = sys.argv[arg_idx+1]
            continue
        if arg == "--computeSimilarity":
            computeSimilarityStr = (sys.argv[arg_idx+1])
            if computeSimilarityStr == "False":
                computeSimilarity = False
            continue
        if arg == "--removeDuplicates":
            removeDuplicatesStr = (sys.argv[arg_idx+1])
            if removeDuplicatesStr == "False":
                removeDuplicates = False
            continue


def die():
    print "Please input the required parameters"
    print "Usage: genClusters.py --input <input filename> --output <output filename> [--separator <sep=\\t>] [--computeSimilarity <True=default|False>] [--threshold <threshold for similarity score to be in one cluster>]"
    exit(1)

args = parse_args()
if inputFilename is None or outputFilename is None:
    die()

print "Generate Clusters: ", inputFilename, ", computeSimilarity:", computeSimilarity, ", scoreThreshold:", scoreThreshold, ", removeDuplicates:", removeDuplicates
sys.stdout.flush()
clusterGen = ClusterGenerator()

clusterGen.run(inputFilename, outputFilename, separator, computeSimilarity, scoreThreshold)
if removeDuplicates:
    for filename in clusterGen.getOutputfilenames():
        clusterGen.removeDuplicates(filename)




