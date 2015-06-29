dig-lsh-clustering
==================

Clustering documents based on LSH


Requirements:
-------------
Download and unzip spark in ```<spark-folder>```

You can run the clustering using a One Step driver - runLSH.py or using 3 steps - tokenization, hashing and then clustering

Tokenization, LSH, Clustering using one step
--------------------------------------------
```
runLSH.py [options] inputFile configFile outputDir
```

To view all options, you can pass --help to the programs. Example:
```
./bin/spark-submit  ~/github/dig-lsh-clustering/runLSH.py --help
```

Example Invocation:
```
 zip -r lsh.zip tokenizer hasher clusterer
 cd <spark-folder>
./bin/spark-submit \
    --master local[*] \
    --py-files ~/github/dig-lsh-clustering/lsh.zip \
    ~/github/dig-lsh-clustering/runLSH.py \
    --base ~/github/dig-lsh-clustering/datasets/geonames/sample.tsv \
    --numHashes 50 --numItemsInBand 5 \
    --computeSimilarity \
    ~/github/dig-lsh-clustering/datasets/sample-ad-location/sample.tsv \
    ~/github/dig-lsh-clustering/datasets/city_state_country_config.json \
    ~/github/dig-lsh-clustering/datasets/sample-ad-location/geonames-clusters
```
The output is in text file format. If you wish to generate the output as
a text file, pass ```--outputformat sequence``` as a parameter


Running tokenization, LSH, clustering Step-by-Step
--------------------------------------------------
Step 1: Tokenization
---------------------
```
tokenizer.py [options] inputFile configFile outputDir
```

Example Invocation:
```
cd tokenizer
zip -r tokenizer.zip RowTokenizer.py inputParser
cd <spark-folder>
./bin/spark-submit \
    --master local[*] \
    --py-files ~/github/dig-lsh-clustering/tokenizer/tokenizer.zip \
    ~/github/dig-lsh-clustering/tokenizer/tokenizer.py \
    ~/github/dig-lsh-clustering/datasets/sample-ad-location/sample.tsv \
    ~/github/dig-lsh-clustering/datasets/city_state_country_config.json \
    ~/github/dig-lsh-clustering/datasets/sample-ad-location/tokens

./bin/spark-submit \
    --master local[*] \
    --py-files ~/github/dig-lsh-clustering/tokenizer/tokenizer.zip \
    ~/github/dig-lsh-clustering/tokenizer/tokenizer.py \
    ~/github/dig-lsh-clustering/datasets/geonames/sample.tsv \
    ~/github/dig-lsh-clustering/datasets/city_state_country_config.json \
    ~/github/dig-lsh-clustering/datasets/geonames/tokens
```

Step 2: Compute LSH
---------------------
```
hasher.py [options] inputDir outputDir
```

Example Invocation:
```
cd hasher
zip -r hasher.zip lsh
cd <spark-folder>
./bin/spark-submit \
    --master local[*] \
    --py-files ~/github/dig-lsh-clustering/hasher/hasher.zip \
    ~/github/dig-lsh-clustering/hasher/hasher.py \
    --saveMinhashes --numHashes 50 --numItemsInBand 5 \
    ~/github/dig-lsh-clustering/datasets/sample-ad-location/tokens \
    ~/github/dig-lsh-clustering/datasets/sample-ad-location/hashes

./bin/spark-submit \
    --master local[*] \
    --py-files ~/github/dig-lsh-clustering/hasher/hasher.zip \
    ~/github/dig-lsh-clustering/hasher/hasher.py \
    --saveMinhashes --numHashes 50 --numItemsInBand 5 \
    ~/github/dig-lsh-clustering/datasets/geonames/tokens \
    ~/github/dig-lsh-clustering/datasets/geonames/hashes
```
You can omit the --saveMinhashes parameter if you do not want a similarity score

Step 3: Perform the clustering
------------------------------
```
clusterer.py [options] inputDir outputDir
```

Example Invocation:
```
./bin/spark-submit \
     --master local[*] \
    ~/github/dig-lsh-clustering/clusterer/clusterer.py \
    --base ~/github/dig-lsh-clustering/datasets/geonames/hashes \
    --computeSimilarity \
    ~/github/dig-lsh-clustering/datasets/sample-ad-location/hashes \
    ~/github/dig-lsh-clustering/datasets/sample-ad-location/geonames-clusters
```
