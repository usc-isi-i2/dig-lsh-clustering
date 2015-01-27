__author__ = 'dipsy'

import sys

def die():
    print "Please input the required parameters"
    print "Usage: str_to_int_tokens.py <input filename> <output filename> <hastable filename> [<token separator=\\t>]"
    exit(1)

def write_tokens(tokens, sep):
    outStr = ""
    usep = ""
    for token in tokens:
        outStr = outStr + usep + str(token)
        usep = sep
    return outStr

if len(sys.argv) < 4:
    die()


inputFile = sys.argv[1]
outputFile = sys.argv[2]
hashFile = sys.argv[3]

separator = "\t"

if len(sys.argv) > 4:
    separator = sys.argv[4]

inFP = open(inputFile, "r")
outFP = open(outputFile, "w")
hashMap = {}
tokenNum = 1
for line in inFP:
    idx = line.find("\t")
    if idx != -1:
        key = line[0:idx]
        line_len = len(line)
        data = line[idx + 1:line_len-1] #remove the \n

        tokens = data.split(separator)
        intTokens = []
        if len(tokens) > 0:
            for token in tokens:
                token = token.strip()
                if len(token) > 0:
                    if hashMap.has_key(token):
                        intTokens.append(hashMap.get(token))
                    else:
                        hashMap[token] = tokenNum
                        intTokens.append(tokenNum)
                        tokenNum +=1

        outFP.write(key + "\t" + write_tokens(intTokens, separator) + "\n")
inFP.close()
outFP.close()

hashFP = open(hashFile, "w")
for token in hashMap.keys():
    hashFP.write(token + "\t" + str(hashMap.get(token)) + "\n")
hashFP.close()
