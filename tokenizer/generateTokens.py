__author__ = 'rajagopal067'
import sys
import ngram
import unicodedata
import re
import json
import codecs

#returns the tokens removing the stop words
def tokenize_input(input):
        tokens = input.split()
        for token in tokens:
            if not token in STOP_WORDS:
                yield token

def write_tokens(tokens, sep):
        outStr = ""
        usep = ""
        if isinstance(tokens,str):
            return tokens
        elif isinstance(tokens,list):
            for token in tokens:
                outStr = outStr + usep + token
                usep = sep
        return outStr


def getNGrams(text,type,n):
        #removes the stop words
        tokens = list(tokenize_input(text))
        if(type == "word"):
            if(len(tokens) > n):
                return ["".join(j) for j in zip(*[tokens[i:] for i in range(n)])]
            else:
            #returns the word directly if n is greater than number of words
                a=[]
                a.append(text)
                return a
        if(type == "character"):
            ngramObject = ngram.NGram(N=n)
            ngram_char_tokens = list(ngramObject.split(text))
            ## remove first n-1 and last n-1 tokens as they are not complete they have $ signs
            if len(text) > n:
                return ngram_char_tokens[n-1:(len(ngram_char_tokens)-(n-1))]
            else:
                a = []
                a.append(text)
                return a
        else:
            return list(tokenize_input(text))

# does the preprocessing takes fieldvalue as input-
# does regex evaluations specified in configuration file, converts to utf8, lowercase
#returns the tokens character or word
def doPreprocessing(dict,fieldvalue):
    try:
        if('replacements' in dict):
            for regex in dict['replacements']:
                text = re.sub(regex['regex'], regex['replacement'], fieldvalue,flags=re.UNICODE)
            if('filters' in dict):
                for filter in dict['filters']:
                    if(('latinfilter' in filter) and (filter['latinfilter'] == True)):
                        text = unicodedata.normalize('NFKD', "".join(text))
                    if(('lowercase' in filter) and (filter['lowercase'] == True)):
                        text = text.lower()
                    if(('uppercase' in filter) and (filter['uppercase'] == True)):
                        text = text.upper()
                    if(('removemultiplespaces' in filter) and (filter['removemultiplespaces']==True)):
                        text = ' '.join(text.split())

            if('tokenization' in dict):
                for param in dict['tokenization']:
                    if('size' in param):
                        size = param["size"]
                    if('type' in param):
                        type = param["type"]
            return getNGrams(text,type,size)
    except KeyError,e:
        print "Keyerror " % str(e)


def parse_args():
    global inputFilename
    global outputFilename
    global separator
    global configurationFilename
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
        if arg == "--config":
            configurationFilename = sys.argv[arg_idx+1]

def die():
    print "Please input the required parameters"
    print "Usage: generateTokensNew.py --input <input filename> --config <configuration filename> --output <output filename> [--separator <sep=\\t>]"
    exit(1)

inputFilename = None
configurationFilename = None
outputFilename = None
separator = "\t"


parse_args()

if len(sys.argv) < 3:
     die()

file = codecs.open(inputFilename,'r',"utf-8")
outputFile = codecs.open(outputFilename,'w','utf-8')
configurationFile = open(configurationFilename)
data = json.load(configurationFile)
STOP_WORDS = data['stopwords']

for line in file:
    lineParts = list(line.strip().split(separator))
    key = ""
    usep=""
    ngram_tokens = []
    for index,fieldvalue in enumerate(lineParts):
        key = key + fieldvalue
        key = re.sub('[^\w\s]', '', key)
        key = ''.join(key.split())
        if (data['config'][index]['defaultConfig'] == True):
            ngram_tokens = ngram_tokens + doPreprocessing(data['defaultConfig']['analyzer'],fieldvalue)
        else:
            ngram_tokens = ngram_tokens + doPreprocessing(data['config'][index]['analyzer'],fieldvalue)

    outputFile.write(key.lower() + separator +write_tokens(ngram_tokens,separator) + "\n")

file.close()
outputFile.close()