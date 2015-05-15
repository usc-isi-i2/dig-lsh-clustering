__author__ = 'rajagopal067'
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import util
import fileinput

similarityList=[]
def findSimilaritySingleFile(LSHKey,keyHashesArray):
    i=0
    while i< len(keyHashesArray):
        j=i+1
        while j < len(keyHashesArray):
            findSimilarityUtil(LSHKey,keyHashesArray[i],keyHashesArray[j])
            j=j+1
        i=i+1

def findSimilarity(LSHKey,keysInput,keysBase):
    if len(keysBase) == 0:
        findSimilaritySingleFile(LSHKey,keysInput)
    else:
        i=0
        while i<len(keysInput):
            j=0
            while j<len(keysBase):
                findSimilarityUtil(LSHKey,keysInput[i],keysBase[j])
                j=j+1
            i=i+1

def findSimilarityUtil(LSHKey,list1,list2):
    key1 = list1[0]
    key2=list2[0]

    hashlist1 = []
    hashlist2 = []
    hashlist1 = list1[1:len(list1)]
    hashlist2 = list2[1 : len(list2)]

    similarity = float(len(set(hashlist1) & set(hashlist2)))/float(len(set(hashlist1)))
    tmpList=[LSHKey,key1,key2,similarity]

    similarityList.append(tmpList)

def write_ToFile():
    outFP = open(outputFilename,"w")
    i=0
    similaritySet=set(map(tuple,similarityList))
    similarityList1=map(list,similaritySet)

    while i<len(similarityList1):
        list1=similarityList1[i]
        string = list1[0] + separator + list1[1] + separator + list1[2] + separator + str(list1[3])+"\n"
        outFP.write(string)
        i=i+1

inputFilename = None
baseFilename = None
inputPrefix = None
basePrefix = None
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

def die():
    print "Please input the required parameters"
    print "Usage: findSimilarity.py --input <input filename> [--inputPrefix <prefix value> --base <base filename> --basePrefix <prefix value>]--output <output filename> [--separator <sep=\\t>] "
    exit(1)

args = parse_args()
if inputFilename is None or outputFilename is None:
    die()

if baseFilename is not None:
    file_list = [inputFilename,baseFilename]
    tmp_combined_file =  inputFilename + ".tmp"
    with open(tmp_combined_file, 'w') as file:
        input_lines = fileinput.input(file_list)
        file.writelines(input_lines)
    util.sort_csv_file(tmp_combined_file, [0], '\t')
    file=open(tmp_combined_file,'r')
else:
    file=open(inputFilename,'r')

previousLSHKey = None
lshKey = None
keysInput=[]
keysBase=[]


for line in file:
    lineParts = line.strip().split("\t")
    lshKey=lineParts[0]
    itemId=lineParts[1]
    idx = line.find("\t")
    line_len = len(line)
    data = line[idx +1 : line_len -1].strip()
    if len(data) > 0:
        tokens = data.split(separator)

    if lshKey != previousLSHKey and previousLSHKey is not None:
        findSimilarity(previousLSHKey,keysInput,keysBase)
        del keysInput[:]
        del keysBase[:]

    if len(tokens) > 0:
        if basePrefix is not None and itemId.startswith(basePrefix):
            keysBase.append(tokens)
        else:
            keysInput.append(tokens)
    previousLSHKey=lshKey

file.close()
findSimilarity(previousLSHKey,keysInput,keysBase)
write_ToFile()

