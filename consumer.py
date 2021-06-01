from kafka import KafkaConsumer
from dotenv import load_dotenv
from pymongo import MongoClient
import json
import os

load_dotenv()

topic_name = os.environ['TOPIC_NAME']
kafka_server = os.environ['KAFKA_SERVER']
db_host = os.environ['DB_HOST']
db_port = int(os.environ['DB_PORT'])
db_name = os.environ['DB_NAME']
db_collection = os.environ['DB_COLLECTION']

client = MongoClient(db_host, db_port)
db = client[db_name]

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[kafka_server],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    auto_commit_interval_ms=5000,
    fetch_max_bytes=128,
    max_poll_records=100,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for message in consumer:
    tweet = json.loads(json.dumps(message.value))
    db[db_collection].insert_one(tweet)
    print(tweet)
