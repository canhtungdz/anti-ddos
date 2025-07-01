import json
import pyshark
import time
from kafka import KafkaProducer

# Kết nối Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Mở file pcap
pcap_file = 'SAT-01-12-2018_0817.pcap'
capture = pyshark.FileCapture(pcap_file, use_json=True)

# Hàm chuyển packet sang JSON với cấu trúc cố định
def packet_to_json(pkt):
    def get_attr(layer, attr, default=0):
        try:
            return int(getattr(layer, attr, default))
        except:
            return default

    ip_layer = getattr(pkt, 'ip', None)
    udp_layer = getattr(pkt, 'udp', None)
    tcp_layer = getattr(pkt, 'tcp', None)

    if not ip_layer or (not udp_layer and not tcp_layer):
        return None

    protocol = 17 if udp_layer else 6

    return {
        "timestamp": str(pkt.sniff_time),
        "src_ip": getattr(ip_layer, 'src', ""),
        "dst_ip": getattr(ip_layer, 'dst', ""),
        "length": int(pkt.length),

        "protocol": protocol,
        "src_port": get_attr(udp_layer or tcp_layer, 'srcport', 0),
        "dst_port": get_attr(udp_layer or tcp_layer, 'dstport', 0),

        "udp_len": get_attr(udp_layer, 'length', 0),
        "tcp_seq": get_attr(tcp_layer, 'seq', 0),
        "tcp_ack": get_attr(tcp_layer, 'ack', 0),
        "tcp_len": get_attr(tcp_layer, 'len', 0),

        "cwr_flag": get_attr(tcp_layer, 'flags_cwr', 0),
        "ece_flag": get_attr(tcp_layer, 'flags_ece', 0),
        "urg_flag": get_attr(tcp_layer, 'flags_urg', 0),
        "ack_flag": get_attr(tcp_layer, 'flags_ack', 0),
        "psh_flag": get_attr(tcp_layer, 'flags_push', 0),
        "rst_flag": get_attr(tcp_layer, 'flags_reset', 0),
        "syn_flag": get_attr(tcp_layer, 'flags_syn', 0),
        "fin_flag": get_attr(tcp_layer, 'flags_fin', 0)
    }

# Đọc và lưu toàn bộ packet kèm timestamp
packets = []
for pkt in capture:
    try:
        data = packet_to_json(pkt)
        if data:
            packets.append((pkt.sniff_time.timestamp(), data))  # timestamp dạng float
    except Exception as e:
        print(f"Bỏ qua gói lỗi: {e}")
capture.close()

print(f"✅ Đã load {len(packets)} gói tin từ file pcap.")

# Gửi gói tin theo thời gian gốc
if packets:
    start_time = packets[0][0]  # thời gian bắt đầu của gói đầu tiên
    real_start = time.time()    # thời gian thực tại khi bắt đầu gửi

    for pkt_time, data in packets:
        # Tính thời gian cần đợi để mô phỏng thời gian thực
        wait_time = (pkt_time - start_time) - (time.time() - real_start)
        if wait_time > 0:
            time.sleep(wait_time)

        try:
            producer.send('ddos_packets_raw3', value=data)
            print(f"📤 Gửi gói tại thời điểm: {data['timestamp']}")
        except Exception as e:
            print(f"❌ Lỗi khi gửi gói: {e}")

    producer.flush()
    print("🎉 Gửi toàn bộ gói tin hoàn tất.")
else:
    print("❗ Không có gói tin hợp lệ nào.")
