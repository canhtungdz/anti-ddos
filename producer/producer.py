import json
import time
from kafka import KafkaProducer
import pandas as pd
import os

FILE_PATH = "/data/raw-packets.json"

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

if not os.path.exists(FILE_PATH):
    print(f"❌ File {FILE_PATH} không tồn tại.")
    exit(1)

df = pd.read_json(FILE_PATH)
if "timestamp" in df.columns:
    df["timestamp"] = df["timestamp"].astype(str)

for idx, row in df.iterrows():
    message = row.to_dict()
    producer.send("ddos_packets_raw", value=message)
    print(f"✔ Gửi dòng {idx + 1}: {message}")
    time.sleep(0.01)

producer.flush()
producer.close()

