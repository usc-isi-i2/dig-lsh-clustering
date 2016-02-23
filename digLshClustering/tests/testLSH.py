#!/usr/bin/env python


try:
    from pyspark import SparkContext
except:
    print "### NO PYSPARK"
import json
import argparse
import sys
from digTokenizer.tokenizer import Tokenizer
from optparse import OptionParser
from digSparkUtil.dictUtil import as_dict, dict_minus
from digLshClustering import Hasher
from digLshClustering import Clusterer
from digLshClustering.clusterer.unionFind import UnionFind


from digSparkUtil.fileUtil import FileUtil

def testLSH(sc, inputFilename,outputFilename,configFilename,
             **kwargs):

    '''
    kwargs is a dictionary of inputs a sample input would look like
    options = {
               "file_format":"sequence",
               "data_type":"json",
               "numHashes":100 ,
               "numItemsInBand": 10,
               "computeSimilarity": True,
               "threshold":0.8,
               "base":"saam-city.json",
               "topk":3,
               "candidatesName":"candidates")
               }
    '''
    futil = FileUtil(sc)


    #Tokenize
    ######################
    rdd_input = futil.load_file(inputFilename, file_format=kwargs['file_format'], data_type="json")
    rdd_input.setName('rdd_input')

    tokenizer = Tokenizer(configFilename, **kwargs)
    rdd_tokenized = tokenizer.perform(rdd_input)

    futil = FileUtil(sc)
    outOptions = {}

    #you can save the tokens file here by using
    #futil.save_file(rdd_tokenized,outputFilename,file_format='sequence',data_type='json',**outOptions)

    #Hashing
    #######################

    hasher = Hasher(**kwargs)
    rdd_minHashes = hasher.perform(rdd_tokenized)
    rdd_minHashes.setName('rdd_minhashes')
    #futil.save_file(rdd_minHashes,outputFilename,file_format='sequence',data_type='json',**outOptions)


    #clustering
    #########################
    clusterer = Clusterer(**kwargs)
    rdd_clusters = clusterer.perform(rdd_minHashes)
    #futil.save_file(rdd_clusters,outputFilename,file_format='text',data_type='json',**outOptions)

    #unionfind
    #########################
    unionFind = UnionFind(**kwargs)
    rdd_unionfind = unionFind.perform(rdd_clusters)

    # SAVE DATA
    futil.save_file(rdd_unionfind,outputFilename,file_format='text',data_type='json',**outOptions)

def main():



    parser = argparse.ArgumentParser()

    parser.add_argument('-i','--inputFile', required=True)
    parser.add_argument('-o','--output_dir', required=True)
    parser.add_argument('--config', default=None, required=True)
    parser.add_argument('--file_format', default='sequence', choices=('text', 'sequence'))

    parser.add_argument("-n","--numHashes",type=int,help="number of minhashes", default=100)
    parser.add_argument("-b","--numItemsInBand",type=int,help="number of items in each band", default=10)

    parser.add_argument("-s","--computeSimilarity",action="store_true",help="compute similarity", default=True)
    parser.add_argument("-t","--threshold",type=float,help="similarity threshold", default=0.0)
    parser.add_argument("-e","--base",help="base file", default="")
    parser.add_argument("-k","--topk", type=int,help="top n matches", default=3)
    parser.add_argument("-z","--candidates_name",help="name for json element for matching candidates", default="candidates")

    args = parser.parse_args()
    kwargs = dict_minus(as_dict(args), "inputFile", "output_dir", "config")

    print kwargs,type(kwargs)
    sc = SparkContext(appName="LSH-HASHER")
    testLSH(sc,args.inputFile,args.output_dir,args.config,**kwargs)

if __name__ == "__main__":
    sys.exit(main())
