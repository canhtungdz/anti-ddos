from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
import json
import os

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "ddos_result")
ES_HOST = os.getenv("ES_HOST", "http://elasticsearch:9200")
ES_INDEX = os.getenv("ES_INDEX", "ddos_flows")

def create_index(es, index_name):
    mapping = {
        "mappings": {
            "properties": {
                "timestamp": {"type": "date", "format": "epoch_second"}
            }
        }
    }
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name, body=mapping)
        print(f"✅ Created index: {index_name}")
    else:
        print(f"ℹ️ Index {index_name} already exists.")

if __name__ == "__main__":
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="indexer-group"
    )

    es = Elasticsearch(ES_HOST)
    create_index(es, ES_INDEX)

    print("📥 Listening to Kafka topic:", KAFKA_TOPIC)
    for msg in consumer:
        doc = msg.value
        try:
            es.index(index=ES_INDEX, document=doc)
            print("✅ Indexed document:", doc.get("flow_id", "(no id)"))
        except Exception as e:
            print("❌ Failed to index document:", e)
