/usr/lib/spark/bin/spark-submit \
    --master yarn-client \
    --py-files lsh.zip \
    preprocess.py \
    $@