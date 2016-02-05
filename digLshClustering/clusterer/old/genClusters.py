__author__ = 'dipsy'
import sys
import os
from hdfs.hfile import Hfile

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import util

class ClusterGenerator(object):

    def __init__(self):
        self.separator = "\t"
        self.score_threshold = 0.0

    def __open_file_for_write(self, file_handle):
        if file_handle.startswith("hdfs://"):
            path = file_handle[7:]
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
            print "Open file: ", file_handle
            sys.stdout.flush()
            return open(file_handle, 'w')

    def run(self, input_filename, output_filename, p_separator, compute_similarity, p_score_threshold):
        self.separator = p_separator
        self.score_threshold = p_score_threshold

        key_hashes = []
        prev_lsh_key = None
        prev_lsh_band = None

        in_file = open(input_filename, 'r')

        out = self.__open_file_for_write(output_filename)
        for line in in_file:
            line = line.strip()
            if len(line) > 0:
                line_tokens = line.split(separator)
                lsh_key = line_tokens[0]
                lsh_band = lsh_key[0:3]

                key_hash = line_tokens[1:]

                if prev_lsh_key is None:
                    prev_lsh_key = lsh_key
                    prev_lsh_band = lsh_band
                    print "Start clustering for Band:", lsh_band
                    sys.stdout.flush()

                if prev_lsh_key != lsh_key:
                    if len(key_hashes) > 1:
                        if compute_similarity:
                            self.__compute_similarity(key_hashes, out, lsh_band)
                        else:
                            self.__write_clusters(key_hashes, out)
                    del key_hashes[:]

                if prev_lsh_band != lsh_band:
                    print "Start clustering for Band:", lsh_band
                    sys.stdout.flush()

                prev_lsh_key = lsh_key
                prev_lsh_band = lsh_band

                key_hashes.append(key_hash)

        if len(key_hashes) > 1:
            if compute_similarity:
                self.__compute_similarity(key_hashes, out, lsh_band)
            else:
                self.__write_clusters(key_hashes, out)
        in_file.close()
        out.close()
        print "Done computing similarities"
        sys.stdout.flush()

    def __write_clusters(self, key_hashes_array, output_file):
        key_arr = []
        for key_arr1 in key_hashes_array:
            key1 = key_arr1[0]
            key_arr.append(key1)

        output_file.write(util.write_tokens(key_arr, self.separator) + "\n")

    def __compute_similarity(self, key_hashes_array, output_file, lsh_band):
        #print "Compute Similarity between: ", len(keyHashesArray), " items"
        num_lines = 0
        for keyArr1 in key_hashes_array:
            key1 = keyArr1[0]
            min_arr1 = keyArr1[1:]
            #print "Start: ", key1
            for keyArr2 in key_hashes_array:
                key2 = keyArr2[0]
                if key1 < key2:
                    min_arr2 = keyArr2[1:]
                    if min_arr1 != min_arr2:
                        score = util.compute_list_similarity(min_arr1, min_arr2)
                    else:
                        score = 1.0
                    if score >= self.score_threshold:
                        if score < 1.0 or lsh_band == "000":
                            output_file.write(key1 + self.separator + key2 + self.separator + str(score) + "\n")
                            num_lines += 1
        return num_lines

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
            scoreThreshold = float(sys.argv[arg_idx+1])
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

parse_args()
if inputFilename is None or outputFilename is None:
    die()

print "Generate Clusters: ", inputFilename, ", computeSimilarity:", computeSimilarity, ", scoreThreshold:", scoreThreshold, ", removeDuplicates:", removeDuplicates
sys.stdout.flush()
clusterGen = ClusterGenerator()

clusterGen.run(inputFilename, outputFilename, separator, computeSimilarity, scoreThreshold)
if removeDuplicates:
    clusterGen.remove_duplicates(outputFilename)




