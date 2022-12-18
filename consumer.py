#!/usr/bin/python3

import json
import pandas as pd
from time import sleep
from google.cloud import bigquery
from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

schema_registry_client = SchemaRegistryClient({'url': 'http://localhost:8081'})
schema = schema_registry_client.get_latest_version('bitcoin_price-topic')
avro_deserializer = AvroDeserializer(schema_str=schema.schema.schema_str, schema_registry_client=schema_registry_client)

consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'auto.offset.reset': 'earliest',
    'group.id': 'test-group',
}
consumer = Consumer(consumer_conf)
consumer.subscribe(['bitcoin_price'])

client = bigquery.Client()
client.create_dataset('data_fellowship', exists_ok=True)
dataset = client.dataset('data_fellowship')

schema = [
    bigquery.SchemaField('Date', 'STRING'),
    bigquery.SchemaField('Open', 'STRING'),
    bigquery.SchemaField('High', 'STRING'),
    bigquery.SchemaField('Low', 'STRING'),
    bigquery.SchemaField('Close', 'STRING'),
    bigquery.SchemaField('Volume', 'STRING'),
    bigquery.SchemaField('Market_Cap', 'STRING'),
]

table_ref = bigquery.TableReference(dataset, 'bitcoin_price')
table = bigquery.Table(table_ref, schema=schema)
client.create_table(table, exists_ok=True)

while True:
    msg = consumer.poll(0)
    if msg is not None:
        data = avro_deserializer(msg.value(), SerializationContext('bitcoin_price', 'topic'))
        print(data)
        client.insert_rows_from_dataframe(table, pd.DataFrame([data]))
        print('[INFO] Data Loaded to BigQuery')