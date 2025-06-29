from kafka import KafkaProducer
import json
import time

# Kafka topic và server
TOPIC = "ddos_packets_raw"
KAFKA_SERVER = "kafka:9092"
# KAFKA_SERVER = "localhost:29092"  # Thay đổi theo cấu hình của bạn

# Tạo Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Đọc danh sách gói tin từ file JSON
with open("kafka_producer/packets.json", "r", encoding="utf-8") as f:
    packets = json.load(f)

# Sắp xếp theo thời gian gửi
packets.sort(key=lambda x: x["timestamp"])

# Gửi từng packet theo thời gian tương đối
start_time = packets[0]["timestamp"]
for i, packet in enumerate(packets):
    if i > 0:
        delay = packet["timestamp"] - packets[i - 1]["timestamp"]
        time.sleep(max(delay, 0))

    producer.send(TOPIC, value=packet)
    print(f"Gửi packet {i+1}: {packet}")

producer.flush()
producer.close()
