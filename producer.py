#!/usr/bin/python3

from time import sleep
from csv import reader
from uuid import uuid4
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

def delivery_report(err, msg):
    if err:
        print('[INFO] Message Delivery Failed')
    else:
        print(f'[INFO] Message Delivered to {msg.topic()} [{msg.partition()}] @{msg.offset()}')

file = open('source/bitcoin_price_training.csv')
csv_reader = reader(file, quotechar='"')
header = next(csv_reader)

schema_registry_client = SchemaRegistryClient({'url': 'http://localhost:8081'})
with open('source/bitcoin_price_schema.avsc') as avsc:
    schema = avsc.read()
avro_serializer = AvroSerializer(schema_str=schema, schema_registry_client=schema_registry_client)
string_serializer = StringSerializer('utf_8')

producer = Producer({'bootstrap.servers': 'localhost:9092'})

for data in csv_reader:
    producer.poll(0)
    print(dict(zip(header, data)))
    producer.produce(
        topic='bitcoin_price', 
        key=string_serializer(str(uuid4()), 'random'), 
        value=avro_serializer(dict(zip(header, data)), 
        SerializationContext('bitcoin_price', 'topic')), 
        on_delivery=delivery_report
    )
    sleep(1)

producer.flush()