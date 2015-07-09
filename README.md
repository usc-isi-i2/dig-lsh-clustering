dig-lsh-clustering
==================

Clustering documents based on LSH


Requirements:
-------------
* Spark: Visit http://spark.apache.org/downloads.html, select the package type of “Pre-built for Hadoop 2.4 and later,” and then click on the link for “Download Spark” This will download a compressed TAR file, or tarball. Uncompress the file into ```<spark-folder>```.

You can run the clustering using a One Step driver - runLSH.py or using 3 steps - tokenization, hashing and then clustering

* Run `./make-spark.sh` every time to build the zip files required by spark every time you pull in new code

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
 cd <spark-folder>
./bin/spark-submit \
    --master local[*] \
    --executor-memory=4g \
    --driver-memory=4g \
    --py-files ~/github/dig-lsh-clustering/lsh.zip \
    ~/github/dig-lsh-clustering/runLSH.py \
    --numPartitions 100 \
    --base ~/github/dig-lsh-clustering/datasets/geonames/sample.tsv \
    --baseConfig ~/github/dig-lsh-clustering/datasets/city_state_country_config.json \
    --numHashes 50 --numItemsInBand 5 \
    --computeSimilarity \
    ~/github/dig-lsh-clustering/datasets/sample-ad-location/sample.tsv \
    ~/github/dig-lsh-clustering/datasets/city_state_country_config.json \
    ~/github/dig-lsh-clustering/datasets/sample-ad-location/geonames-clusters
```
* The output is in text file format. If you wish to generate the output as
a text file, pass ```--outputformat sequence``` as a parameter
* It by default returns the results as json lines. To return them as csv, pass ```--outputtype csv```
* For each source, it return top 3 candidates (More if there is a tie on score). To increase the number of result candiates pass ```--topk 10```. Pass the value as -1 to return all results.

To cluster a dataset (i.e. not against a base dataset, but to find clusters within itself),
omit the --base parameter while clustering. Also the input can be a sequenceFile, and that can be specified
by --inputformat sequence. The data type can be json (the config file must contain "path" to each field that should be
extracted) that is specified by --type json

Example Invocation:
```
./bin/spark-submit \
    --master local[*] \
    --executor-memory=4g \
    --driver-memory=4g \
    --py-files ~/github/dig-lsh-clustering/lsh.zip \
    ~/github/dig-lsh-clustering/runLSH.py \
    --numHashes 50 --numItemsInBand 5 \
    --computeSimilarity \
    --type json --inputformat sequence \
    ~/github/dig-lsh-clustering/datasets/body_text/sample-ad.seq \
    ~/github/dig-lsh-clustering/tokenizer/sample_json_config.json \
    ~/github/dig-lsh-clustering/datasets/body_text/seq_clusters
```

Sometime there are a lot of duplicates in the dataset, and computing similarities can prove to be very expensive.
In this case, we can create 2 datasets - one defining identical clusters, and the second defining similarities
between these identical clusters. To do so, pass --computeIdenticalClusters to the clusterer
Example Invocation:
```
./bin/spark-submit \
    --master local[*] \
    --executor-memory=4g \
    --driver-memory=4g \
    --py-files ~/github/dig-lsh-clustering/lsh.zip \
    ~/github/dig-lsh-clustering/runLSH.py \
    --numHashes 50 --numItemsInBand 5 \
    --computeSimilarity --computeIdenticalClusters \
    ~/github/dig-lsh-clustering/datasets/body_text/istr-100k/body.tsv \
    ~/github/dig-lsh-clustering/datasets/body_text/config.json \
    ~/github/dig-lsh-clustering/datasets/body_text/istr-100k/clusters
```

Running tokenization, LSH, clustering Step-by-Step
--------------------------------------------------
Step 1: Tokenization
---------------------
```
tokenizer.py [options] inputFile configFile outputDir
```

Example Invocation:
```
cd <spark-folder>
./bin/spark-submit \
    --master local[*] \
    --executor-memory=4g \
    --driver-memory=4g \
    --py-files ~/github/dig-lsh-clustering/tokenizer/tokenizer.zip \
    ~/github/dig-lsh-clustering/tokenizer/tokenizer.py \
    ~/github/dig-lsh-clustering/datasets/sample-ad-location/sample.tsv \
    ~/github/dig-lsh-clustering/datasets/city_state_country_config.json \
    ~/github/dig-lsh-clustering/datasets/sample-ad-location/tokens

./bin/spark-submit \
    --master local[*] \
    --executor-memory=4g \
    --driver-memory=4g \
    --py-files ~/github/dig-lsh-clustering/tokenizer/tokenizer.zip \
    ~/github/dig-lsh-clustering/tokenizer/tokenizer.py \
    ~/github/dig-lsh-clustering/datasets/geonames/sample.tsv \
    ~/github/dig-lsh-clustering/datasets/city_state_country_config.json \
    ~/github/dig-lsh-clustering/datasets/geonames/tokens
```

To tokenize a sequence file containing json data, the config file must contain "path" to each field that should be
extracted.
```
./bin/spark-submit \
    --master local[*] \
    --executor-memory=4g \
    --driver-memory=4g \
    --py-files ~/github/dig-lsh-clustering/tokenizer/tokenizer.zip \
    ~/github/dig-lsh-clustering/tokenizer/tokenizer.py \
    --type json --inputformat sequence \
    ~/github/dig-lsh-clustering/datasets/body_text/sample-ad.seq \
    ~/github/dig-lsh-clustering/tokenizer/sample_json_config.json \
    ~/github/dig-lsh-clustering/datasets/body_text/seq_tokens
```

Step 2: Compute LSH
---------------------
```
hasher.py [options] inputDir outputDir
```

Example Invocation:
```
cd <spark-folder>
./bin/spark-submit \
    --master local[*] \
    --executor-memory=4g \
    --driver-memory=4g \
    --py-files ~/github/dig-lsh-clustering/hasher/hasher.zip \
    ~/github/dig-lsh-clustering/hasher/hasher.py \
    --saveMinhashes --numHashes 50 --numItemsInBand 5 \
    ~/github/dig-lsh-clustering/datasets/sample-ad-location/tokens \
    ~/github/dig-lsh-clustering/datasets/sample-ad-location/hashes

./bin/spark-submit \
    --master local[*] \
    --executor-memory=4g \
    --driver-memory=4g \
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
     --executor-memory=4g \
    --driver-memory=4g \
    ~/github/dig-lsh-clustering/clusterer/clusterer.py \
    --base ~/github/dig-lsh-clustering/datasets/geonames/hashes \
    --computeSimilarity \
    ~/github/dig-lsh-clustering/datasets/sample-ad-location/hashes \
    ~/github/dig-lsh-clustering/datasets/sample-ad-location/geonames-clusters
```

* The output is in text file format. If you wish to generate the output as
a text file, pass ```--outputformat sequence``` as a parameter
* It by default returns the results as json lines. To return them as csv, pass ```--outputtype csv```
* For each source, it return top 3 candidates (More if there is a tie on score). To increase the number of result candiates pass ```--topk 10```. Pass the value as -1 to return all results.


#Troubleshooting
----------------
1. <b>Am getting "OutOfMemoryError".</b>

 Pass parameter --numPartitions and set a big value. example: --numPartitions 10000
 Also, try to increase the executor-memory and driver-memory for Spark.

 2. <b>How can I see all the options for a command.</b>

 Pass --help to the command to see all the options. Example:
 ```
./bin/spark-submit  ~/github/dig-lsh-clustering/runLSH.py --help
```
