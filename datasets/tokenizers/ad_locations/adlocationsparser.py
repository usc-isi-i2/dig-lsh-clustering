import json
import sys

def getValues(dict):
    str=""
    usep=""
    if 'featureObject' in dict:
        if 'addressLocality' in dict['featureObject']:
            str = str + usep + dict['featureObject']['addressLocality']
            usep=separator
        if 'addressRegion' in dict['featureObject']:
            str = str + usep + dict['featureObject']['addressRegion']
            usep=separator
        if 'addressCountry' in dict['featureObject'] and 'label' in dict['featureObject']['addressCountry']:
            str = str + usep + dict['featureObject']['addressCountry']['label']
        if 'addressCountry' in dict['featureObject'] and isinstance(dict['featureObject']['addressCountry'],list):
            maxstr=""
            for index,value in enumerate(dict['featureObject']['addressCountry']):
                if 'label' in value:
                    str = value['label']
                if len(maxstr) > len(str):
                    maxstr = str

    return str

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
    print "Usage: adlocationsparser.py --input <input filename> --output <output filename> [--separator <sep=\\t>]"
    exit(1)



inputFilename = None
outputFilename = None
separator = "\t"
parse_args()
if len(sys.argv) < 2:
    die()

file = open(inputFilename,'r')
outputFile = open(outputFilename,'w')


for line in file:
    data = json.loads(line)
    if 'location' in data and isinstance(data['location'],dict):
        str = getValues(data['location'])
        outputFile.write(str + "\n")
    else:
        maxstr=""
        for index,value in enumerate(data['location']):
            str = getValues(data['location'][index])
            if len(str) > len(maxstr):
                maxstr = str
        outputFile.write(maxstr + "\n")

file.close()
outputFile.close()
