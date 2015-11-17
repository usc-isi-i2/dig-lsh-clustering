dig-lsh-clustering
==================

Clustering documents based on LSH


Requirements:
-------------
* Spark: Visit http://spark.apache.org/downloads.html, select the package type of “Pre-built for Hadoop 2.4 and later,” and then click on the link for “Download Spark” This will download a compressed TAR file, or tarball. Uncompress the file into ```<spark-folder>```.

You can run the clustering using a One Step driver - runLSH.py or using 3 steps - tokenization, hashing and then clustering

* Run `./make-spark.sh` every time to build the zip files required by spark every time you pull in new code


Running tokenization, LSH, clustering Step-by-Step
--------------------------------------------------
Step 1: Tokenization
---------------------
See: https://github.com/usc-isi-i2/dig-tokenizer
Generate the tokens with `--outputformat sequence` to generate the tokens as a sequence file that the LSH hasher can use

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

Step 4: Union the clusters
---------------------------
This is an optional step to compute union of 2 clusters. This is used  only if clusters are generated <b>without</b> a base.
If there is are clusters: [{A, B, C}, {B, D}], then this will union them and produce a single cluster [{A, B, C, D}]

```
unionFind.py [options] inputDir outputDir
```

Example Invocation:
```
./bin/spark-submit \
     --master local[*] \
     --executor-memory=4g \
    --driver-memory=4g \
    ~/github/dig-lsh-clustering/clusterer/unionFind.py \
    --inputtype json --inputformat sequence --outputtype json --outputformat sequence \
    ~/github/dig-lsh-clustering/datasets/ad-tiles/clusters \
    ~/github/dig-lsh-clustering/datasets/ad-tiles/clusters-union
```

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
