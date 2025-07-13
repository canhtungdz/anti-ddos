from elasticsearch import Elasticsearch, helpers
import json
import os

ES_HOST = "http://elasticsearch:9200"
NDJSON_PATH = "/app/data/stream_output/batch_13.ndjson"
INDEX_NAME = "ddos_flows2"
def load_ndjson(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                yield {
                    "_index": INDEX_NAME,
                    "_source": json.loads(line)
                }

def create_index_with_mapping(es, index_name):
    # Kiểm tra nếu index chưa tồn tại thì tạo
    if not es.indices.exists(index=index_name):
        mapping = {
            "mappings": {
                "properties": {
                    "timestamp": {
                        "type": "date",
                        "format": "strict_date_optional_time||epoch_second"
                    }
                }
            }
        }
        es.indices.create(index=index_name, body=mapping)
        print(f"✅ Created index '{index_name}' with timestamp mapping.")
    else:
        print(f"⚠️ Index '{index_name}' already exists.")

if __name__ == "__main__":
    es = Elasticsearch(ES_HOST)
    
    # Bước 1: Tạo index và mapping nếu chưa có
    create_index_with_mapping(es, INDEX_NAME)
    
    # Bước 2: Upload dữ liệu
    try:
        helpers.bulk(es, load_ndjson(NDJSON_PATH))
        print(f"✅ Uploaded NDJSON to Elasticsearch index '{INDEX_NAME}'")
    except Exception as e:
        print(f"❌ Upload failed: {e}")