/usr/lib/spark/bin/spark-submit \
    --master yarn-client \
    --py-files /home/ubuntu/dig-lsh-clustering/tokenizer/tokenizer.zip \
    /home/ubuntu/dig-lsh-clustering/tokenizer/tokenizer.py \
    hdfs://memex-nn1:8020/view/user/worker/lsh-clustering/geonames/allcountries_pop.tsv  \
    hdfs://memex-nn1:8020/view/user/worker/lsh-clustering/geonames/geonames_config.json \
    hdfs://memex-nn1:8020/view/user/worker/lsh-clustering/geonames/tokens


rm -rf ~/Downloads/test_data/geonames/tokens; ./bin/spark-submit \
    --master local[*] \
    --executor-memory=12g \
    --driver-memory=12g \
    --py-files ~/github/dig-lsh-clustering/tokenizer/tokenizer.zip \
    ~/github/dig-lsh-clustering/tokenizer/tokenizer.py \
    ~/Downloads/test_data/geonames/geonames.tsv  \
    ~/Downloads/test_data/geonames/config_geonames.json \
    ~/Downloads/test_data/geonames/tokens

rm -rf ~/Downloads/test_data/saam/hashes; ./bin/spark-submit \
    --master local[*] \
    --executor-memory=12g \
    --driver-memory=12g \
    --py-files ~/github/dig-lsh-clustering/hasher/hasher.zip \
    ~/github/dig-lsh-clustering/hasher/hasher.py \
    --saveMinhashes --numHashes 50 --numItemsInBand 5 \
    ~/Downloads/test_data/saam/tokens \
    ~/Downloads/test_data/saam/hashes

rm -rf ~/Downloads/test_data/geonames/hashes; ./bin/spark-submit \
    --master local[*] \
    --executor-memory=12g \
    --driver-memory=12g \
    --py-files ~/github/dig-lsh-clustering/hasher/hasher.zip \
    ~/github/dig-lsh-clustering/hasher/hasher.py \
    --saveMinhashes --numHashes 50 --numItemsInBand 5 \
    ~/Downloads/test_data/geonames/tokens \
    ~/Downloads/test_data/geonames/hashes

rm -rf ~/Downloads/test_data/saam/geonames-clusters; ./bin/spark-submit \
     --master local[*] \
     --executor-memory=12g \
    --driver-memory=12g \
    ~/github/dig-lsh-clustering/clusterer/clusterer.py \
    --base ~/Downloads/test_data/geonames/hashes \
    --computeSimilarity \
    ~/Downloads/test_data/saam/hashes \
    ~/Downloads/test_data/saam/geonames-clusters
