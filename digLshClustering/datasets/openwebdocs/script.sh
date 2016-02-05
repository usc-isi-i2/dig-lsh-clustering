rm -rf ~/github/dig-lsh-clustering/datasets/openwebdocs/tokens; ./bin/spark-submit \
    --master local[*] \
    --executor-memory=12g \
    --driver-memory=12g \
    --py-files ~/github/dig-lsh-clustering/tokenizer/tokenizer.zip \
    ~/github/dig-lsh-clustering/tokenizer/tokenizer.py \
    ~/github/dig-lsh-clustering/datasets/openwebdocs/data.csv \
    ~/github/dig-lsh-clustering/datasets/openwebdocs/config.json \
    ~/github/dig-lsh-clustering/datasets/openwebdocs/tokens

# rm -rf ~/github/dig-lsh-clustering/datasets/openwebdocs/tokens-text; ./bin/spark-submit \
#     --master local[1] \
#     --executor-memory=12g \
#     --driver-memory=12g \
#     ~/github/dig-lsh-clustering/sequenceToText.py \
#     ~/github/dig-lsh-clustering/datasets/openwebdocs/tokens \
#     ~/github/dig-lsh-clustering/datasets/openwebdocs/tokens-text

rm -rf ~/github/dig-lsh-clustering/datasets/openwebdocs/hashes; ./bin/spark-submit \
    --master local[*] \
    --executor-memory=12g \
    --driver-memory=12g \
    --py-files ~/github/dig-lsh-clustering/hasher/hasher.zip \
    ~/github/dig-lsh-clustering/hasher/hasher.py \
    --saveMinhashes --numHashes 50 --numItemsInBand 5 \
    ~/github/dig-lsh-clustering/datasets/openwebdocs/tokens \
    ~/github/dig-lsh-clustering/datasets/openwebdocs/hashes

#Do the clustering
rm -rf ~/github/dig-lsh-clustering/datasets/openwebdocs/clusters-50-5; ./bin/spark-submit \
     --master local[*] \
     --executor-memory=12g \
    --driver-memory=12g \
    ~/github/dig-lsh-clustering/clusterer/clusterer.py \
    --computeSimilarity \
    --numPartitions 5 \
    --topk -1 \
    ~/github/dig-lsh-clustering/datasets/openwebdocs/hashes \
    ~/github/dig-lsh-clustering/datasets/openwebdocs/clusters-50-5

