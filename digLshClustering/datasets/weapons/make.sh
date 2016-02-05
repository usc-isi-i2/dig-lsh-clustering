cd tokenizer; rm tokenizer.zip; zip -r tokenizer.zip RowTokenizer.py inputParser ngram; cd ..
cd hasher; rm hasher.zip; zip -r hasher.zip lsh; cd ..
zip -r lsh.zip tokenizer hasher clusterer
