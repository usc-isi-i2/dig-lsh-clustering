__author__ = 'rajagopal067'
import sys
import ngram
import unicodedata
import re
import json
import codecs
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def tokenize_input(input):
    tokens = input.split()
    for token in tokens:
        yield token

#returns the tokens removing the stop words
def tokenize_input_stopwords(input, stop_words):
    tokens = input.split()
    for token in tokens:
        if not token in stop_words:
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
        if len(text) > n:
            return ngram_char_tokens
        else:
            a = []
            a.append(text)
            return a
    else:
        return list(tokenize_input(text))

# does the preprocessing takes fieldvalue as input-
# does regex evaluations specified in configuration file, converts to utf8, lowercase
#returns the tokens character or word
def doPreprocessing(text,prefix,analyzer, settings):
    if analyzer.has_key("replacements"):
        for replacement in analyzer["replacements"]:
            text = re.sub(replacement['regex'], replacement['replacement'], text,flags=re.UNICODE)

    if analyzer.has_key("filters"):
        for filter in analyzer["filters"]:
            if filter == "lowercase":
                text = text.lower()
            elif filter == "uppercase":
                text = text.upper()
            elif filter == "latin":
                nfkd_form = unicodedata.normalize('NFKD', "".join(text))
                text = nfkd_form.encode('ASCII','ignore')
            else:
                filter_settings = settings[filter]
                if filter_settings["type"] == "stop":
                    words = filter_settings["words"]
                    tokens = tokenize_input_stopwords(text, words)
                    text = " ".join(tokens)

    tokens = []
    if analyzer.has_key("tokenizers"):
        for tokenizer in analyzer["tokenizers"]:
            if tokenizer == "whitespace":
                tokens.extend(tokenize_input(text))
            else:
                tokenizer_setting = settings[tokenizer]
                if tokenizer_setting["type"] == "character_ngram":
                    size = int(tokenizer_setting["size"])
                    tokens.extend(getNGrams(text, "character", size))
                elif tokenizer_setting["type"] == "word_ngram":
                    size = int(tokenizer_setting["size"])
                    tokens.extend(getNGrams(text, "word", size))

    final_tokens = []
    for token in tokens:
        final_tokens.append(prefix+token)

    return final_tokens


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

def getTokens(line):
    tokens=[]
    for index, fieldvalue in enumerate(line):
        prefix = fieldvalue.split(':')[0] + ':'
        fieldvalue = fieldvalue.split(':')[1]
        token_config={}
        for index,field_config in config["fieldConfig"].iteritems():
            if (field_config.has_key("prefix") and field_config["prefix"] == prefix):
                token_config = field_config
        if token_config.has_key('analyzer'):
            analyzer=token_config["analyzer"]
        else:
            analyzer=config["defaultConfig"]["analyzer"]
        field_tokens = doPreprocessing(fieldvalue,prefix, analyzer, settings)
        tokens.extend(field_tokens)
    return tokens

# returns a dictionary which has boolean values for a blank field token is allowed or not
def getDictionaryValues(config,dict_blank_fields,dict_prefixes):

    for index, fieldvalue in enumerate(lineParts):
        if(config["fieldConfig"].has_key(str(index))):
            field_config = config["fieldConfig"][str(index)]
        else:
            field_config = config["defaultConfig"]
        if field_config.has_key("prefix"):
                dict_prefixes[index] = field_config["prefix"]
        if field_config.has_key("allow_blank"):
            if field_config["allow_blank"] is True:
                dict_blank_fields[index]=True
            else:
                dict_blank_fields[index]=False
        else:
            dict_blank_fields[index]=False


# returns crossproduct based on blank tokens allowed for a field value
def getCrossProduct(dict_blank_fields,dict_prefixes,line):
    crossProduct=[[]]
    temp=[]
    for index in dict_blank_fields.keys():
        if dict_blank_fields[index] is False:
            crossProduct = [x + [dict_prefixes[index] + line[index]] for x in crossProduct]
    for index in dict_blank_fields.keys():
        if dict_blank_fields[index] is True:
            if crossProduct:
                temp = [x+ [dict_prefixes[index] + line[index]] for x in crossProduct]
                crossProduct = crossProduct + temp
    return crossProduct


def die():
    print "Please input the required parameters"
    print "Usage: generateTokens.py --input <input filename> --config <configuration filename> --output <output filename> [--separator <sep=\\t>]"
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
config = json.load(configurationFile)
settings = config["settings"]

for line in file:
    lineParts = list(line.strip().split(separator))
    key = lineParts[0]
    lineParts = lineParts[1:]
    tokens = []
    dict_blank_fields = {}
    dict_prefixes = {}
    getDictionaryValues(config,dict_blank_fields,dict_prefixes)
    crossProductArray = getCrossProduct(dict_blank_fields,dict_prefixes,lineParts)

    for crossProduct in crossProductArray:
        if crossProduct:
            tokens = getTokens(crossProduct)
            outputFile.write(key + separator + write_tokens(tokens, separator) + "\n")

print "Done tokenizing"
file.close()
outputFile.close()