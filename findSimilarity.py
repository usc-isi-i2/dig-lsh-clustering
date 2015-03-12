__author__ = 'rajagopal067'
import sys
similarityList=[]
def findSimilarity(LSHKey,keyHashesArray):
    i=0
    while i< len(keyHashesArray):
        j=i+1
        while j < len(keyHashesArray):
            findSimilarityUtil(LSHKey,keyHashesArray[i],keyHashesArray[j])
            j=j+1
        i=i+1

def findSimilarityUtil(LSHKey,list1,list2):
    key1 = list1[0]
    key2=list2[0]

    hashlist1 = []
    hashlist2 = []
    hashlist1 = list1[1:len(list1)]
    hashlist2 = list2[1 : len(list2)]

    similarity = float(len(set(hashlist1) & set(hashlist2)))/float(len(hashlist1))
    tmpList=[LSHKey,key1,key2,similarity]

    similarityList.append(tmpList)

def write_ToFile():
    outFP = open(outputFilename,"w")
    i=0
    while i<len(similarityList):
        list=similarityList[i]
        string = list[0] + separator + list[1] + separator + list[2] + separator + str(list[3])+"\n"
        outFP.write(string)
        i=i+1

inputFilename = None
outputFilename = None
separator = "\t"
dataType = "integer"

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
    print "Usage: findSimilarity.py --input <input filename> --output <output filename> [--separator <sep=\\t>] "
    exit(1)

args = parse_args()
if inputFilename is None or outputFilename is None:
    die()

file=open(inputFilename,'r')

keyHashes=[]
counter = 0
LSHKey = 0
prev_LSHKey = "null"


for line in file:
    idx = line.find("\t")
    if idx !=-1:
        LSHKey = line[0:idx]
        line_len = len(line)
        data = line[idx +1 : line_len -1].strip()
        if len(data) > 0:
            tokens = data.split(separator)
        if len(tokens) > 0:
            if (LSHKey == prev_LSHKey or prev_LSHKey=="null"):
                keyHashes.append(tokens)
            else:
                findSimilarity(prev_LSHKey,keyHashes)
                keyHashes=[]
                keyHashes.append(tokens)


    prev_LSHKey = LSHKey
findSimilarity(prev_LSHKey,keyHashes)
write_ToFile()

