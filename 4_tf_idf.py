# -*- coding: utf-8 -*-

import pandas as pd 
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
import matplotlib.pyplot as plt
import numpy as np
#import csv

#csv.field_size_limit(sys.maxsize)

text_df = pd.read_csv("output/troll_detection/authors_citations/ok/textblob.csv")
text_df = text_df.reindex(columns=['authid','text'])


v = TfidfVectorizer(
                    stop_words="english",
                    #max_features=10000
                    )
X = v.fit_transform(text_df['text'])
X_df = pd.DataFrame(X.todense())

output_df = text_df[['authid']].join(pd.DataFrame(X.todense()))
#output_df.to_csv('debug/output_df.csv', sep=',', encoding='utf-8')
true_k = 4
km = KMeans(n_clusters=true_k, init='k-means++', max_iter=300, n_init=1,
                verbose=1)
km.fit(X)
#episodes = defaultdict(list)
#with open("output/sentiment_analysis/authors_citations/ok/textblob.csv", "r") as sentences_file:
#    reader = csv.reader(sentences_file, delimiter=',')
#    next(reader)
#    for row in reader:
#        corpus.append(row) 

print("Top terms per cluster:")
order_centroids = km.cluster_centers_.argsort()[:, ::-1]
terms = v.get_feature_names()
for i in range(true_k):
    print ("Cluster {}:".format(i))
    for ind in order_centroids[i, :10]:
        print (' {}'.format(terms[ind]))
        



pca =  PCA(n_components=2).fit(X.todense())
data2D = pca.transform(X.todense())
colormap = np.array(['red', 'lime', 'black', 'blue','purple','cyan','orange','pink'])

plt.scatter(data2D[:,0], data2D[:,1], c=colormap[km.labels_])
plt.show() 