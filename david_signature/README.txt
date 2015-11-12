This repo has 100k ads file and postprocess script.
I got 100k ads file by converting the sequence file into json lines.


Instructions for running LSH with david's signature:

1. Run LSH as usual using the code here https://github.com/usc-isi-i2/dig-lsh-clustering
for reference i will list the commands here:

Tokenization:
a)./bin/spark-submit --master local[*] --executor-memory=10g --driver-memory=10g --py-files /Users/rajagopal/Desktop/dig-lsh-clustering/tokenizer/tokenizer.zip /Users/rajagopal/Desktop/dig-lsh-clustering/tokenizer/tokenizer.py --type json /Users/rajagopal/Desktop/github_repos/dig-unicode/ht_data/LSH/newLSH_data/10k-new-ht-data-1GB-signs.jl /Users/rajagopal/Desktop/github_repos/dig-unicode/ht_data/LSH/newLSH_data/tokenizer.json /Users/rajagopal/Desktop/github_repos/dig-unicode/ht_data/LSH/tokens_body

Hashing:
b)./bin/spark-submit --master local[*] --executor-memory=10g --driver-memory=10g --py-files /Users/rajagopal/Desktop/dig-lsh-clustering/hasher/hasher.zip /Users/rajagopal/Desktop/dig-lsh-clustering/hasher/hasher.py --saveMinhashes --numHashes 50 --numItemsInBand 5 /Users/rajagopal/Desktop/github_repos/dig-unicode/ht_data/LSH/tokens_body/ /Users/rajagopal/Desktop/github_repos/dig-unicode/ht_data/LSH/hashes_body

c) clustering:
./bin/spark-submit --executor-memory=10g --driver-memory=10g /Users/rajagopal/Desktop/dig-lsh-clustering/clusterer/clusterer.py --computeSimilarity --topk -1 /Users/rajagopal/Desktop/github_repos/dig-unicode/ht_data/LSH/hashes_body/ /Users/rajagopal/Desktop/github_repos/dig-unicode/ht_data/LSH/clusters_body

2) after getting clusters combine all the small files into one big file using cat part-00000* > clusters.txt

3) clusters.txt has just uri's, to add description and title run the postprocess script 

arguments for postprocess.py:
a) clusters.txt
b) ads file 
c) outputfile 