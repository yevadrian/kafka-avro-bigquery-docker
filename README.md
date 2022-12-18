### Using Apache Kafka and Confluent Schema Registry to stream data into Google BigQuery

This project includes the dataset and the script needed to load the source data into the Kafka topic.
Messages from the file are serialized into Avro format, then sent into the Kafka Topic by the producer.
The consumer will consume the messages and deserialize it into its original format.
The deserialized messages are then loaded into your Google BigQuery table.

##### Clone this repository and enter the directory
```bash
git clone https://github.com/yevadrian/kafka-avro-bigquery-docker && cd kafka-avro-bigquery-docker
```

##### Create Kafka stacks with Docker Compose
```bash
sudo docker compose up -d
```

##### Edit the service-account.json according to your Google Cloud credentials
```json
{
  "type": "service_account",
  "project_id": "[PROJECT_ID]",
  "private_key_id": "[KEY_ID]",
  "private_key": "-----BEGIN PRIVATE KEY-----\n[PRIVATE_KEY]\n-----END PRIVATE KEY-----\n",
  "client_email": "[SERVICE_ACCOUNT_EMAIL]",
  "client_id": "[CLIENT_ID]",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://accounts.google.com/o/oauth2/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/[SERVICE_ACCOUNT_EMAIL]"
}
```

##### Install required Python packages
```bash
pip install -r requirements.txt
```

##### Run the producer to stream the data into the Kafka topic
```bash
python3 producer.py
```

##### Run the consumer to consume the data from Kafka topic and load them into BigQuery
```bash
python3 consumer.py
```

##### Open Confluent Control Center to monitor topic and schema registry
```
localhost:9021
```

##### Producer succesfully produce data
![Producer](https://user-images.githubusercontent.com/110159876/208313550-5317498e-3848-4418-9858-e8a90d425842.jpg)

##### Consumer successfully consume data and load to BigQuery
![Consumer](https://user-images.githubusercontent.com/110159876/208313637-0d69b28e-02fd-4dbf-adc0-c18b2c208b7c.jpg)
