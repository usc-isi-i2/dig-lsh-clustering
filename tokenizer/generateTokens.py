__author__ = 'rajagopal067'
import sys
import ngram
import unicodedata
import re

class TokenPreprocessor:

    STOP_WORDS = ["a", "an", "and", "are", "as", "at", "be", "but", "by",
"for", "if", "in", "into", "is", "it",
"no", "not", "of", "on", "or", "such",
"that", "the", "their", "then", "there", "these",
"they", "this", "to", "was", "will", "with", "-", ";", ",", "_", "+", "/", "\\"]


    def getPreprocessedTokens(self,text,compute_n_gram_characters,compute_n_gram_words,n):
        #removes the stop words
        tokens = list(self.tokenize_input(text))
        if compute_n_gram_words:
            return ["".join(j) for j in zip(*[tokens[i:] for i in range(n)])]
        if compute_n_gram_characters:
            ngramObject = ngram.NGram(N=n)
            tokenized_text = "".join(str(j) for j in tokens)
            ngram_char_tokens = list(ngramObject.split(tokenized_text))
            ## remove first n-1 and last n-1 tokens as they are not complete they have $ signs
            return ngram_char_tokens[n-1:(len(ngram_char_tokens)-(n-1))]
        else:
            return list(self.tokenize_input(text))

    def tokenize_input(self,input):
        tokens = str(input).split()
        for token in tokens:
            if not token in TokenPreprocessor.STOP_WORDS:
                yield token

    def write_tokens(self,tokens, sep):
        outStr = ""
        usep = ""
        for token in tokens:
            outStr = outStr + usep + token
            usep = sep
        return outStr

    def convert_to_LowerCase(self,text):
        return text.lower()

    def convert_UTF8_toAscii(self,text):
        return unicodedata.normalize('NFKD', text).encode('ascii','ignore')

    def remove_special_characters(self,text):
        return re.sub('[^\w\s]', '', str(text))

    def replace_All_White(self,text):
        return ' '.join(text.split())

def parse_args():
    global inputFilename
    global outputFilename
    global separator
    global n_gram_words
    global n_gram_characters
    global n_gram_size

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
        if arg == "--computengramwords":
            n_gram_words = sys.argv[arg_idx+1]
            continue
        if arg == "--computengramcharacters":
            n_gram_characters = sys.argv[arg_idx+1]
            continue
        if arg=="--ngramsize":
            n_gram_size = sys.argv[arg_idx+1]
            continue


inputFilename = None
outputFilename = None
n_gram_words = None
n_gram_characters = None
n_gram_size = 2
separator = "\t"

def die():
    print "Please input the required parameters"
    print "Usage: GenerateTokens.py --input <input filename> --output <output filename> [--separator <sep=\\t>] [--computengramcharacters <True or False>] [--computengramwords <True or False>] " \
          "[--ngramsize <size>]"
    exit(1)


parse_args()

if len(sys.argv) < 3:
    die()

file = open(inputFilename,'r')
outputFile = open(outputFilename,'w')
for line in file:
    lineParts = list(line.strip().split("\t"))
    tokenPreprocessor = TokenPreprocessor();
    preprocessedText = tokenPreprocessor.remove_special_characters(lineParts)
    ngram_tokens = TokenPreprocessor().getPreprocessedTokens(preprocessedText,n_gram_characters,n_gram_words,int(n_gram_size))
    outputFile.write(TokenPreprocessor().write_tokens(ngram_tokens,"\t") + "\n")

file.close()
outputFile.close()






