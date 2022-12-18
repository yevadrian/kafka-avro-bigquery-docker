#!/usr/bin/python3

from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

schema_registry_client = SchemaRegistryClient({'url': 'http://localhost:8081'})
schema = schema_registry_client.get_latest_version('bitcoin_price-topic')

avro_deserializer = AvroDeserializer(schema_str=schema.schema.schema_str, schema_registry_client=schema_registry_client)
conf = {
    'bootstrap.servers': 'localhost:9092',
    'auto.offset.reset': 'earliest',
    'group.id': 'test-group',
}
consumer = Consumer(conf)

consumer.subscribe(['bitcoin_price'])

while True:
    msg = consumer.poll(5)
    if msg is None:
        print('No message to deliver')
        continue
    user = avro_deserializer(msg.value(), SerializationContext('bitcoin_price', 'topic'))
    print(user)