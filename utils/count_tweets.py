import os, base64, re, logging, time, json
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic

def kafka_message_count(topic):
  # To consume messages
  consumer = KafkaConsumer(topic,
                           auto_commit_interval_ms= 30 * 1000,
                           auto_offset_reset='earliest',
                           bootstrap_servers = ['localhost:9092'],
                           consumer_timeout_ms=1000)

  count = 0

  for message in consumer:
    count += 1
  print(f'Number of messages in topic "{topic}": {count}')

if __name__ == '__main__':
  kafka_message_count('important_tweets')
