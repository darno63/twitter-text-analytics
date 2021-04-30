from bonsai import elasticsearch_connect
import pandas as pd

body = [{'index': 'important_tweets'}, {"size" : 10}]
es = elasticsearch_connect()
res = es.msearch(body)

# print(type((res['responses'])[0]))
# print(res['responses'][0]['hits']['hits'][0]['_source']['text'])
# print(res['responses'][0]['hits']['hits'][0]['_source'])
x = res['responses'][0]['hits']['hits'][0]['_source']

df = pandas.DataFrame(x)
print(df)
