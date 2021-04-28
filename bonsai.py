import os, base64, re, logging, time, json
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic

# Load environmental variables from .env file
load_dotenv()

# Log transport details (optional):
logging.basicConfig(level=logging.INFO)

# Parse the auth and host from env:
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
    port=443
  # Connect to cluster over SSL using auth for best security:
  es_header = [{
  'host': host,
  'port': port,
  'use_ssl': True,
  'http_auth': (auth[0],auth[1])
  }]
  # Instantiate the new Elasticsearch connection:
  es = Elasticsearch(es_header)
  # Verify that Python can talk to Bonsai (optional):
  es.ping()
  return es

def elasticsearch_consumer_main():
  es = elasticsearch_connect()
  # To consume messages
  consumer = KafkaConsumer('trump',
                           auto_commit_interval_ms= 30 * 1000,
                           auto_offset_reset='earliest',
                           bootstrap_servers = ['localhost:9092'])

  esid = -1

  for message in consumer:
    time.sleep(1)
    print('next')
    esid += 1
    if esid % 1000 == 0:
      print(esid)

    msg = json.loads(message.value)

    msg_id = msg['id']
    print(msg)

    res = es.index(index="tweets", id=msg_id, body=msg)




if __name__ == '__main__':
  elasticsearch_consumer_main()