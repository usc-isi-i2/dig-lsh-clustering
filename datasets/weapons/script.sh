#Prepare the weapons dataset

rm -rf /Volumes/dipsy/isi/lsh/weapons/input; ./bin/spark-submit \
    --master local[*] \
    --executor-memory=12g \
    --driver-memory=12g \
    --py-files ~/github/dig-lsh-clustering/lsh.zip \
    ~/github/dig-lsh-clustering/datasets/weapons/preprocess.py \
    hdfs://memex-nn1:8020/user/worker/process/atf/weapons/trial03 \
    ~/github/dig-lsh-clustering/datasets/weapons/weapons_config.json \
    /Volumes/dipsy/isi/lsh/weapons/input

rm -rf /Volumes/dipsy/isi/lsh/weapons/tokens; ./bin/spark-submit \
    --master local[*] \
    --executor-memory=12g \
    --driver-memory=12g \
    --py-files ~/github/dig-lsh-clustering/tokenizer/tokenizer.zip \
    ~/github/dig-lsh-clustering/tokenizer/tokenizer.py \
    /Volumes/dipsy/isi/lsh/weapons/input \
    ~/github/dig-lsh-clustering/datasets/weapons/config_city_state.json \
    /Volumes/dipsy/isi/lsh/weapons/tokens

rm -rf /Volumes/dipsy/isi/lsh/weapons/tokens-text; ./bin/spark-submit \
    --master local[1] \
    --executor-memory=12g \
    --driver-memory=12g \
    ~/github/dig-lsh-clustering/sequenceToText.py \
    /Volumes/dipsy/isi/lsh/weapons/tokens \
    /Volumes/dipsy/isi/lsh/weapons/tokens-text

rm -rf /Volumes/dipsy/isi/lsh/weapons/hashes; ./bin/spark-submit \
    --master local[*] \
    --executor-memory=12g \
    --driver-memory=12g \
    --py-files ~/github/dig-lsh-clustering/hasher/hasher.zip \
    ~/github/dig-lsh-clustering/hasher/hasher.py \
    --saveMinhashes --numHashes 50 --numItemsInBand 5 \
    /Volumes/dipsy/isi/lsh/weapons/tokens \
    /Volumes/dipsy/isi/lsh/weapons/hashes

#Prepare the geonames dataset
rm -rf /Volumes/dipsy/isi/lsh/geonames/tokens; ./bin/spark-submit \
    --master local[*] \
    --executor-memory=12g \
    --driver-memory=12g \
    --py-files ~/github/dig-lsh-clustering/tokenizer/tokenizer.zip \
    ~/github/dig-lsh-clustering/tokenizer/tokenizer.py \
    ~/github/dig-lsh-clustering/datasets/geonames/us_populated_places.tsv \
    ~/github/dig-lsh-clustering/datasets/weapons/config_city_state.json \
    /Volumes/dipsy/isi/lsh/geonames/tokens

rm -rf /Volumes/dipsy/isi/lsh/geonames/tokens-text; ./bin/spark-submit \
    --master local[1] \
    --executor-memory=12g \
    --driver-memory=12g \
    ~/github/dig-lsh-clustering/sequenceToText.py \
    /Volumes/dipsy/isi/lsh/geonames/tokens \
    /Volumes/dipsy/isi/lsh/geonames/tokens-text

# ./bin/spark-submit \
#     --master local[1] \
#     --executor-memory=12g \
#     --driver-memory=12g \
#     ~/github/dig-lsh-clustering/count_keys.py \
#     /Volumes/dipsy/isi/lsh/weapons/tokens

rm -rf /Volumes/dipsy/isi/lsh/geonames/hashes; ./bin/spark-submit \
    --master local[*] \
    --executor-memory=12g \
    --driver-memory=12g \
    --py-files ~/github/dig-lsh-clustering/hasher/hasher.zip \
    ~/github/dig-lsh-clustering/hasher/hasher.py \
    --saveMinhashes --numHashes 50 --numItemsInBand 5 \
    /Volumes/dipsy/isi/lsh/geonames/tokens \
    /Volumes/dipsy/isi/lsh/geonames/hashes



#Do the clustering
rm -rf /Volumes/dipsy/isi/lsh/weapons/clusters-3gm-50-5; ./bin/spark-submit \
     --master local[*] \
     --executor-memory=12g \
    --driver-memory=12g \
    ~/github/dig-lsh-clustering/clusterer/clusterer.py \
    --base /Volumes/dipsy/isi/lsh/geonames/hashes \
    --computeSimilarity \
    --numPartitions 1000 \
    --candidatesName geonames_addresses \
    --topk -1 \
    /Volumes/dipsy/isi/lsh/weapons/hashes \
    /Volumes/dipsy/isi/lsh/weapons/clusters-3gm-50-5


#Create HIVE JOIN for joining the geonames dataset
export HADOOP_HEAPSIZE=4096
export HADOOP_CLIENT_OPTS=-Xmx4196m
export HADOOP_HOME=~/hive-join/hadoop-2.6.0
export HIVE_HOME=~/hive-join/apache-hive-1.1.0-bin
export PATH=${HIVE_HOME}:$PATH
export HIVE_OPTS='--hiveconf mapred.job.tracker=local --hiveconf fs.default.name=file:///tmp \
    --hiveconf hive.metastore.warehouse.dir=file:///tmp/warehouse \
    --hiveconf javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=/tmp/metastore_db;create=true'

~/hive-join/apache-hive-1.1.0-bin/bin/hive -f ~/hive-join/scripts/all_in_one_join.hql  \
    -d TABLE_NAME=merged  \
    -d TABLE_DIRECTORY=/Volumes/dipsy/isi/lsh/weapons/clusters-3gm-50-5-merged \
    -d JSON_PATH_TO_MERGE=$.geonames_addresses \
    -d TARGET_TABLE_NAME=target \
    -d TARGET_TABLE_DIRECTORY=/Volumes/dipsy/isi/lsh/weapons/clusters-3gm-50-5 \
    -d SOURCE_TABLE_NAME=source \
    -d SOURCE_TABLE_DIRECTORY=/Volumes/dipsy/isi/lsh/geonames/jsonld \
    -d JSON_PATH_FOR_MERGE_URIS=$.geonames_addresses.uri \
    -d PATH_TO_JAR=~/hive-join/lib/karma-mr-0.0.1-SNAPSHOT-shaded.jar \
    -d ATID=uri \
    -v

#Convert /Volumes/dipsy/isi/lsh/weapons/clusters-3gm-50-5-merged to Sequence file using CreateSequenceFile in Karma
#Upload to Hue: /user/worker/lsh-clustering/weapons/clusters-3gm-50-5-merge-side1
#Rune coordinate-2-dirs workflow with DATASET1: /user/worker/process/atf/weapons/trial03/ and DATASET2: /user/worker/lsh-clustering/weapons/clusters-3gm-50-5-merge-side1
#and OUTPUT_DIR as /user/worker/lsh-clustering/weapons/clusters-3gm-50-5

./bin/spark-submit \
    --master local[1] \
    --executor-memory=12g \
    --driver-memory=12g \
    ~/github/dig-lsh-clustering/sequenceToText.py \
    hdfs://memex-nn1:8020/user/worker/lsh-clustering/weapons/clusters-4gm-50-5-FINAL \
    /Volumes/dipsy/isi/lsh/weapons/clusters-4gm-50-5-FINAL \
    --values

