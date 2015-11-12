import json
import ast
import sys
dictionary = {}

def postprocess(clustersFileName,outputFilePath):
    inputFile = open(clustersFileName,'r')
    outputFile = open(outputFilePath,'w')

    for line in inputFile:
        jsonObj = ast.literal_eval(line)
        newjsonObj = []
        for ad in jsonObj['cluster']:
                if 'description' in dictionary[ad['uri']]:
                    ad['description'] = dictionary[ad['uri']]['description']
                if 'signature' in dictionary[ad['uri']]:
                    ad['signature'] = dictionary[ad['uri']]['signature']
                if 'url' in dictionary[ad['uri']]:
                    ad['url'] = dictionary[ad['uri']]['url']
                if 'title' in dictionary[ad['uri']]:
                    ad['title'] = dictionary[ad['uri']]['title']
                if 'score' not in ad or 'score' in ad and ad['score'] >=0.75:
                    newjsonObj.append(ad)
        outputFile.write(json.dumps(newjsonObj)+"\n")



def readInputFile(inputFilePath):
    inputFile = open(inputFilePath,'r')
    for line in inputFile:
        jsonObj = json.loads(line)
        dictionary[jsonObj['uri']] = {}
        if 'description' in jsonObj:
            dictionary[jsonObj['uri']]['description'] = jsonObj['description']
        if 'signature' in jsonObj:
            dictionary[jsonObj['uri']]['signature'] = jsonObj['signature']
        if 'uri' in jsonObj:
            dictionary[jsonObj['uri']]['uri'] = jsonObj['uri']
        if 'url' in jsonObj:
            dictionary[jsonObj['uri']]['url'] = jsonObj['url']
        if 'title' in jsonObj['mainEntity']:
            dictionary[jsonObj['uri']]['title'] = jsonObj['mainEntity']['title']



if __name__ == '__main__':
    clustersFileName = '/Users/rajagopal/Desktop/github_repos/dig-unicode/ht_data/LSH/clusters-body/clusters.txt'
    inputFilePath = "/Users/rajagopal/Desktop/github_repos/dig-unicode/ht_data/100k-new-ht-data-1GB.jl"
    outputFilePath = '/Users/rajagopal/Desktop/github_repos/dig-unicode/ht_data/LSH/postpro-clusters-50-10-whitespace-unic.jl'
    readInputFile(inputFilePath)
    postprocess(clustersFileName,outputFilePath)