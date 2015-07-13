#Tokenize the files
rm -rf /Volumes/dipsy/isi/lsh/geonames/tokens; ./bin/spark-submit \
    --master local[*] \
    --executor-memory=12g \
    --driver-memory=12g \
    --py-files ~/github/dig-lsh-clustering/tokenizer/tokenizer.zip \
    ~/github/dig-lsh-clustering/tokenizer/tokenizer.py \
    ~/github/dig-lsh-clustering/datasets/geonames/us_populated_places.tsv \
    ~/github/dig-lsh-clustering/datasets/weapons/geonames_config.json \
    /Volumes/dipsy/isi/lsh/geonames/tokens


rm -rf /Volumes/dipsy/isi/lsh/weapons/tokens; ./bin/spark-submit \
    --master local[*] \
    --executor-memory=12g \
    --driver-memory=12g \
    --py-files ~/github/dig-lsh-clustering/tokenizer/tokenizer.zip \
    ~/github/dig-lsh-clustering/tokenizer/tokenizer.py \
    --type json --inputformat sequence \
    hdfs://memex-nn1:8020/user/worker/process/atf/weapons/trial03 \
    ~/github/dig-lsh-clustering/datasets/weapons/weapons_config.json \
    /Volumes/dipsy/isi/lsh/weapons/tokens

# ./bin/spark-submit \
#     --master local[1] \
#     --executor-memory=12g \
#     --driver-memory=12g \
#     ~/github/dig-lsh-clustering/count_keys.py \
#     /Volumes/dipsy/isi/lsh/weapons/tokens

# ./bin/spark-submit \
#     --master local[1] \
#     --executor-memory=12g \
#     --driver-memory=12g \
#     ~/github/dig-lsh-clustering/sequenceToText.py \
#     /Volumes/dipsy/isi/lsh/weapons/tokens \
#     /Volumes/dipsy/isi/lsh/weapons/tokens-text

#Hash the files
rm -rf /Volumes/dipsy/isi/lsh/geonames/hashes; ./bin/spark-submit \
    --master local[*] \
    --executor-memory=12g \
    --driver-memory=12g \
    --py-files ~/github/dig-lsh-clustering/hasher/hasher.zip \
    ~/github/dig-lsh-clustering/hasher/hasher.py \
    --saveMinhashes --numHashes 50 --numItemsInBand 5 \
    /Volumes/dipsy/isi/lsh/geonames/tokens \
    /Volumes/dipsy/isi/lsh/geonames/hashes

rm -rf /Volumes/dipsy/isi/lsh/weapons/hashes; ./bin/spark-submit \
    --master local[*] \
    --executor-memory=12g \
    --driver-memory=12g \
    --py-files ~/github/dig-lsh-clustering/hasher/hasher.zip \
    ~/github/dig-lsh-clustering/hasher/hasher.py \
    --saveMinhashes --numHashes 50 --numItemsInBand 5 \
    /Volumes/dipsy/isi/lsh/weapons/tokens \
    /Volumes/dipsy/isi/lsh/weapons/hashes

#Do the clustering
rm -rf /Volumes/dipsy/isi/lsh/weapons/clusters-3gm-50-5; ./bin/spark-submit \
     --master local[*] \
     --executor-memory=12g \
    --driver-memory=12g \
    ~/github/dig-lsh-clustering/clusterer/clusterer.py \
    --base /Volumes/dipsy/isi/lsh/geonames/hashes \
    --computeSimilarity \
    --numPartitions 1000 \
    /Volumes/dipsy/isi/lsh/weapons/hashes \
    /Volumes/dipsy/isi/lsh/weapons/clusters-3gm-50-5
