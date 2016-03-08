dig-lsh-clustering
==================

dig-lsh-clustering is a python library implementation of [Locality Senstive Hashing](http://infolab.stanford.edu/~ullman/mmds/ch3.pdf) which clusters similar documents based on the tokens.


dig-lsh-clustering will help you:
* Perform clustering based on the tokens generated using config file. You can learm more about tokenizer options here [dig-tokenizer](https://github.com/usc-isi-i2/dig-tokenizer)


Requirements:
-------------
* Spark: Visit http://spark.apache.org/downloads.html, select the package type of “Pre-built for Hadoop 2.4 and later,” and then click on the link for “Download Spark” This will download a compressed TAR file, or tarball. Uncompress the file into ```<spark-folder>```.
* Set SPARK_HOME as environment variable and install pyspark

#### How to install
pip install digLshClustering

#### Usage
*You can run the following command by cloning this repo with the config file and input files like following:
```
python dig-lsh-clustering/digLshClustering/tests/testLSH.py \
      -i dig-lsh-clustering/digLshClustering/tests/text-json/input --file_format text \
      --numHashes 20 --numItemsInBand 10 --computeSimilarity --threshold 0.8 \
      -o dig-lsh-clustering/digLshClustering/tests/text-json/output \
      --config dig-lsh-clustering/digLshClustering/tests/tokenizer.json 
```

Or if you want to import this library into you code check [here](https://github.com/usc-isi-i2/dig-lsh-clustering/blob/master/digLshClustering/tests/testLSH.py) for the usage or  


This library uses [spark-util](https://github.com/usc-isi-i2/dig-sparkutil). This is file util built for spark rdds to read them and save them easily.


#### Examples
To run this code, you have to input a config file which generates tokens. Let's look at an example with the config file present [here](https://github.com/usc-isi-i2/dig-lsh-clustering/blob/master/sample-files/config.json) 
```
{
  "fieldConfig": {
    "0": {
      "path": ".signature",
      "prefix": "",
      "allow_blank": false,
      "analyzer": {
        "tokenizers": [
          "character_n_7"
        ]
      }
    }
  },
  "settings": {
    "character_n_7": {
      "type": "character_ngram",
      "size": 7
    }
  }
}
```
Let's take the input present [here](https://raw.githubusercontent.com/usc-isi-i2/dig-lsh-clustering/master/sample-files/input) 

If you run the clustering using the command
```
python dig-lsh-clustering/digLshClustering/tests/testLSH.py \
      -i dig-lsh-clustering/digLshClustering/tests/text-json/input --file_format text \
      -o dig-lsh-clustering/digLshClustering/tests/text-json/output \
      --config dig-lsh-clustering/digLshClustering/tests/tokenizer.json 
```
you will get the output present [here](https://github.com/usc-isi-i2/dig-lsh-clustering/tree/master/sample-files/output) 

The results get better if your input size increases. Sample input I used here has only 500 lines, so the results might not be great.



#### Options Available :
* numHashes (optional) : Number of Hash Functions to use in the algorithm, default value is 100
* numItemsInBand (optional) : Number of items present in each band, default is 10
* topk (optional) : For each cluster, return the top k items
* threshold (optional) : Put the item in the cluster only if this many number of hashes match. If this value is high, you are making the clustering strict. Default value is specified by the algorithm ( (1/numofbands)^(1/numsItemsInBand) ). This is a float value, so if you want to use this option, you have to give 0.6 for 60% hashes match. 



#### Required Arguments: 
1. Input file on which you want to cluster
2. config file : tokenizer specs -  
check [sample tokenizer] (https://github.com/usc-isi-i2/dig-lsh-clustering/blob/devel/digLshClustering/tests/tokenizer.json)
3. output directory

#### How to run with all the Options Available
```
python  dig-lsh-clustering/digLshClustering/tests/testLSH.py \
        -i dig-lsh-clustering/digLshClustering/tests/text-json/input --file_format text \
        --numHashes 20 --numItemsInBand 10 --computeSimilarity --threshold 0.8 --topk 3\
        -o dig-lsh-clustering/digLshClustering/tests/text-json/output \
        --config dig-lsh-clustering/digLshClustering/tests/tokenizer.json
```
