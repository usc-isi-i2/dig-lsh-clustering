__author__ = 'dipsy'
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import util

# precision =  | {relevant documents} ^ {retrieved documents} |
#             -------------------------------------------------
#                         {retrieved documents}
#
#
# recall =  | {relevant documents} ^ {retrieved documents} |
#           ------------------------------------------------
#                         {relevant documents}

inputFilename = None
truthFilename = None
separator = "\t"

def parse_args():
    global inputFilename
    global truthFilename
    global separator

    for arg_idx, arg in enumerate(sys.argv):
        if arg == "--input":
            inputFilename = sys.argv[arg_idx+1]
            continue
        if arg == "--groundtruth":
            truthFilename = sys.argv[arg_idx+1]
            continue
        if arg == "--separator":
            separator = sys.argv[arg_idx+1]
            continue


def die():
    print "Please input the required parameters"
    print "Usage: computeClusterPrecisionRecall.py --input <input filename> --groundtruth <file containing ground truth> [--separator <sep=\\t>]"
    exit(1)

def get_first_from_line(line):
    global separator
    return int(line.split(separator)[0])

parse_args()
print inputFilename + ", truth:" + truthFilename
if inputFilename is None or truthFilename is None:
    die()


truthFile = open(truthFilename, 'r')
searcher = util.Searcher(inputFilename)

numItems = 0
total_precision = 0
total_recall = 0
for line in truthFile:
    line = line.strip()
    if len(line) > 0:
        lineParts = line.split(separator)
        key = lineParts[0]
        base_matches = lineParts[1:]
        num_relevant_docs = len(base_matches)
        #print "Key:", key
        #print "Base Matches", base_matches

        matches = searcher.find(int(key), key=get_first_from_line)
        num_retrieved_docs = 0
        num_relevant_docs_retrieved = 0
        if matches is not None and len(matches) > 0:
            match = matches[0]
            matchParts = match.split(separator)
            matchParts = matchParts[1:] #remove the key
            num_retrieved_docs = len(matchParts)
            #print "\tMatches", num_retrieved_docs
            #print matchParts
            for base_match in base_matches:
                if base_match in matchParts:
                    num_relevant_docs_retrieved += 1

        precision = 0.0
        if num_retrieved_docs > 0:
            precision = float(num_relevant_docs_retrieved) / num_retrieved_docs

        recall = (num_relevant_docs_retrieved) / num_relevant_docs
        #print key, "> ", num_relevant_docs, num_retrieved_docs, num_relevant_docs_retrieved
        print key + ", precision=" + str(precision) + ", recall=" + str(recall)

        total_precision += precision
        total_recall += recall
        numItems += 1

print "---------------------------------------------"
print "Average Precision:", (total_precision/numItems)
print "Average Recall:", (total_recall/numItems)


