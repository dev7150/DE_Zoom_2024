#!/home/suzu.sharma/anaconda3/envs/kafka/bin/python
from time import sleep
from json import dumps
from kafka import KafkaProducer
import sys

print(sys.path)

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))

for e in range(1000):
    data = {'number' : e}
    producer.send('demo_1', value=data)
    print("producing")
    sleep(1)