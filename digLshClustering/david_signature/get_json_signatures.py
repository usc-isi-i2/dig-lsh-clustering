from sys import argv,exit
import json as JSON
import re
import codecs
from argparse import ArgumentParser

scriptArgs = ArgumentParser()

scriptArgs.add_argument("inputfile",help="Input JSON file, one JSON record per line")
scriptArgs.add_argument("outputfile",help="Output JSON file, one JSON record per line (unless --pretty was specified)")
scriptArgs.add_argument("--pretty",action="store_true",help="Pretty print the JSON output")
scriptArgs.add_argument("--maxlines",help="Maximum number of lines in the input file to process")
scriptArgs.add_argument("--runlimit",help="Limit the number of successive instances of the same character in the signature to this value")

argValues = scriptArgs.parse_args()

inputFile   = argValues.inputfile
outputFile  = argValues.outputfile
prettyPrint = argValues.pretty
maxLines    = int(argValues.maxlines) if argValues.maxlines else None
runLimit    = int(argValues.runlimit) if argValues.runlimit else None

def main ():
    instream  = codecs.open(inputFile,"r","utf-8")
    outstream = codecs.open(outputFile,"w","utf-8")
    lineNum   = 0
    for line in instream:
        try:
            json                      = JSON.loads(line)
            mainEntity = json['mainEntity']
            titles                    = removeDuplicates([preprocessSent(sent) for sent in makeList(mainEntity.get('title'))])
            descriptions              = removeDuplicates([preprocessSent(sent) for sent in makeList(json.get("description"))])
            json["preprocessed"]      = append(titles,descriptions)
            #json["title_signature"]   = getCompleteSignature(titles)
            #json["descrip_signature"] = getCompleteSignature(descriptions)
            json['signature'] = getCompleteSignature(titles) + "   " + getCompleteSignature(descriptions)
            
            if (prettyPrint):
                toStr = JSON.dumps(json,sort_keys=True,indent=4)
            else:
                toStr = JSON.dumps(json,sort_keys=True)
            outstream.write("%s\n" % toStr)
        except ValueError:
            print "Ignoring value error on line %d" % (lineNum+1)
        lineNum += 1
        if (lineNum%1000 ==0):
            print "%d processed" % lineNum
        if (maxLines != None and lineNum == maxLines):
            break
    instream.close()
    outstream.close()
    print "Lines processed: %d" % lineNum


# Isn't there something that already does this?
def append (lst1,lst2):
    result = []
    result.extend(lst1)
    result.extend(lst2)
    return result

def removeDuplicates (lst):
    result = []
    for x in lst:
        if (x not in result):
            result.append(x)
    return result

def preprocessSent (sent):

    """Cleans up the text, removing HTML tags and entities, and other stuff"""

    # Strip enclosing quotes if they are present
    sent = sent.strip('"')

    # link tags with attributes
    sent = re.sub(r'<a .*>', " ",sent)
    sent = re.sub(r'<iframe .*>', " ",sent)

    # User-exposed URLs
    sent = re.sub(r'https?:\S*'," ",sent)

    # HTML open tag: <br>
    sent = re.sub(r'<\s*[a-zA-Z]+\s*>'," ",sent)
    sent = re.sub(r'<\s*\/\s*[a-zA-Z]+\s*>'," ",sent)
    sent = re.sub(r'<\s*[a-zA-Z]+\s*\/\s*>'," ",sent)

    # HTML entities
    sent = sent.replace("&nbsp;", " ")    
    sent = re.sub(r'&lt;br&gt;'," ",sent)
    sent = re.sub(r'&lt;', " ",sent)
    sent = re.sub(r'&gt;', " ",sent)
    sent = re.sub(r'&\S+;', " ",sent)
    # Broken entity
    sent = re.sub(r'&#\d+', " ",sent)
    # Escaped newlines,tabs
    sent = re.sub(r'\n'," ",sent)
    sent = re.sub(r'\t'," ",sent)
    sent = re.sub(r'\r'," ",sent)
    return sent

def limitRunsInSignature (signature):
    currentRun = 0
    currentChar = None
    result = ""
    for i in range(0,len(signature)):
        char = signature[i]
        if (char == currentChar):
            if (currentRun < runLimit):
                currentRun += 1
                result += char
        else:
            currentRun = 1 
            currentChar = char
            result += char
    return result

def getCompleteSignature (descriptions):
    """Takes a list of sentences and returns the concatenated signature for them."""
    signatures = [getSentenceSignature(sent) for sent in makeList(descriptions)]
    complete = "".join(signatures)
    if (runLimit):
        complete = limitRunsInSignature(complete)
    return complete

def getSentenceSignature (sent):
    """Takes a sentence and returns the signature for it."""
    sent = preprocessSent(sent)
    tokens = []
    for tok in sent.split():
        tokens.extend(splitToken(tok))
    sigs = [getTokenSignature(token) for token in tokens]
    return "".join(sigs)

def makeList (value):
    """Takes a value and turns it into a list if it is not one already."""
    if (value == None):
        return []
    elif (type(value) != list):
        return [value]
    else:
        return value


def splitToken (tok):
    """Takes a token without whitespace and splits it into a list of sub-tokens based on character classes."""
    result = []
    while (tok):
        # Letter sequence
        m = re.match(r'^([a-zA-Z]+)(.*)',tok)
        if (m):
            result.append(m.group(1))
            tok = m.group(2)
            continue
        # A sequence of neither digits nor letters
        m = re.match(r'^([^\da-zA-Z]+)(.*)',tok)
        if (m):
            result.append(m.group(1))
            tok = m.group(2)
            continue
        # A sequence of digits
        m = re.match(r'(\d+)(.*)',tok)
        if (m):
            result.append(m.group(1))
            tok = m.group(2)
            continue
        #
        raise RuntimeError("Shouldn't come here")
    return result


def getTokenSignature (tok):
    """Takes a token; returns its corresponding signature."""
    # Single upper-case letter: L
    m = re.match(r'[A-Z]$',tok)
    if (m):
        return "L"
    # Multi-letter word all-caps: W
    if (re.match(r'[A-Z][A-Z]+$',tok)):
        return "W"
    # Capitalized word: first letter upper, subsequent letters all lower-case: C
    if (re.match(r'[A-Z][a-z]+$',tok)):
        return "C"
    # All-lower case word: w
    if (re.match(r'[a-z]+$',tok)):
        return "w"
    # Mixed case word: M
    if (re.match(r'[a-zA-Z]+$',tok)):
        return "M"
    # Digit string is a matching string of D's.
    if (re.match(r'\d+$',tok)):
        result = ""
        for i in range(0,len(tok)):
            result += "D"
        return result
    # Otherwise, the signature is just the token itself.
    return tok



if (__name__ == "__main__"):
    main()
