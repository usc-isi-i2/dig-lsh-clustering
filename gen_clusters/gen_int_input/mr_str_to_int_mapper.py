#!/usr/bin/env python

__author__ = 'dipsy'

import zipimport
importer = zipimport.zipimporter('python-packages.mod')
gensim = importer.load_module('gensim')

import sys
from gensim import corpora
import json
import os
import subprocess

key_dictionary = corpora.Dictionary()
token_dictionary = corpora.Dictionary()



def get_line_representation(self, line):
    global key_dictionary
    global token_dictionary
    all_tokens = line.lower().split('\t')
    key = all_tokens[0]
    tokens = all_tokens[1:]
    yield key_dictionary.token2id.get(key)
    for token in tokens:
        yield token_dictionary.token2id.get(token)


for line in sys.stdin:
    line = line.strip()
    idx = line.find("\t")
    if idx != -1:
        try:
            key = line[0:idx]
            tokens =  line[idx+1:].split('\t')
            key_dictionary.doc2bow([key], allow_update=True)
            token_dictionary.doc2bow(tokens, allow_update=True)

            print get_line_representation(line)
        except Exception as e:
            print >> sys.stderr, e
            pass


#output_dir = os.getenv('mapred_output_dir')
#idx = output_dir.rfind("/")
#output_dir = output_dir[0:idx]
#key_filename = output_dir + '/output-key-' + os.getenv('mapred_task_id')
#key_proc = subprocess.Popen(['hadoop', 'fs', '-put', '-', key_filename], stdin=subprocess.PIPE)
#key_dictionary.save(key_proc.stdin)
#key_proc.stdin.close()
#key_proc.wait()

#token_filename = output_dir + '/output-token-' + os.getenv('mapred_task_id')
#token_proc = subprocess.Popen(['hadoop', 'fs', '-put', '-', token_filename], stdin=subprocess.PIPE)
#token_dictionary.save(token_proc.stdin)
#token_proc.stdin.close()
#token_proc.wait()

