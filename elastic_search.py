import json
import os
import re
import time
from elasticsearch_dsl import Search
from elasticsearch import Elasticsearch
from bonsai import elasticsearch_connect
import pandas as pd
"""
import nltk
from nltk.stem import PorterStemmer
from nltk.corpus import stopwords
nltk.download('punkt')
nltk.download('wordnet')
nltk.download('stopwords')
"""


def gather_data():
  es = elasticsearch_connect()
  s = Search(using=es, index="covid_tweets")
  df = pd.DataFrame([hit.to_dict() for hit in s.scan()])
  # print(df.columns)
  return df

def identify_tokens(row):
  twitter_text = row['text']
  tokens = nltk.word_tokenize(twitter_text)
  token_words = [w for w in tokens if w.isalpha()]
  return token_words

"""
def elasticsearch_connect():
    bonsai = os.environ['BONSAI_URL']
    auth = re.search('https\:\/\/(.*)\@', bonsai).group(1).split(':')
    host = bonsai.replace('https://%s:%s@' % (auth[0], auth[1]), '')
    # optional port
    match = re.search('(:\d+)', host)
    if match:
        p = match.group(0)
        host = host.replace(p, '')
        port = int(p.split(':')[1])
    else:
        port = 443
    # Connect to cluster over SSL using auth for best security:
    es_header = [{
        'host': host,
        'port': port,
        'use_ssl': True,
        'http_auth': (auth[0], auth[1])
    }]
    # Instantiate the new Elasticsearch connection:
    es = Elasticsearch(es_header)
    # Verify that Python can talk to Bonsai (optional):
    es.ping()
    return es
"""

def gather_data():
    es = elasticsearch_connect()
    s = Search(using=es, index="tweets")
    df = pd.DataFrame([hit.to_dict() for hit in s.scan()])
    print(df.columns)
    return df

def identify_tokens(row):
    twitter_text = row['text']
    tokens = nltk.word_tokenize(twitter_text)
    token_words = [w for w in tokens if w.isalpha()]
    return token_words

def stem_list(row):
    stemming = PorterStemmer()
    my_list = row['tokenized']
    stemmed_list = [stemming.stem(word) for word in my_list]
    return stemmed_list

def remove_stops(row):
    stops = set(stopwords.words("english"))
    my_list = row['stemmed']
    meaningful_words = [w for w in my_list if not w in stops]
    return meaningful_words

def rejoin_words(row):
    my_list = row['stopped_removed']
    joined_words = (" ".join(my_list))
    return joined_words

def text_analytics():
    df = gather_data()
    df['tokenized'] = df.apply(identify_tokens, axis=1)
    print("Tokenized")
    print(df.tokenized.to_string(index=False))
    df['stemmed'] = df.apply(stem_list, axis=1)
    print("stemmed")
    print(df.stemmed.to_string(index=False))
    df['stopped_removed'] = df.apply(remove_stops, axis=1)
    print("stopped_removed")
    print(df.stopped_removed.to_string(index=False))
    df['processed'] = df.apply(rejoin_words, axis=1)
    print("processed")
    print(df.processed.to_string(index=False))

if __name__ == '__main__':
  # text_analytics()
  df = gather_data()
  print(df.shape)