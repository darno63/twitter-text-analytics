import requests
import pandas as pd
from kafka import KafkaProducer

import os,base64,re,logging

from elasticsearch import Elasticsearch

logging.basicConfig(level=logging.INFO)

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaConsumer, KafkaProducer
from dotenv import load_dotenv
import json

load_dotenv()

CONSUMER_KEY = os.environ['CONSUMER_KEY']
CONSUMER_SECRET = os.environ['CONSUMER_SECRET']
ACCESS_TOKEN = os.environ['ACCESS_TOKEN']
ACCESS_SECRET = os.environ['ACCESS_SECRET']

"""
admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092", 
    client_id='test'
)

topic_list = ['important_tweets', 'vaccination_tweets', 'covid_tweets']
admin_client.create_topics(new_topics=topic_list, validate_only=False)
"""

kafka_producer = KafkaProducer(bootstrap_servers='localhost:9092')

class StdOutListener(StreamListener):
    def on_data(self, data):
        data_json = json.loads(data)
        if data_json['user']['followers_count'] > 10000:
            kafka_producer.send("trump", data.encode('utf-8')).get(timeout=10)
            print(data)
        return True

    def on_error(self, status):
        print(status)


def kafka_run():
    l = StdOutListener()
    auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
    stream = Stream(auth, l)
    stream.filter(track=["trump", "hillary", "biden"])


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    kafka_run()
