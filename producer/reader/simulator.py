import json
import time
from datetime import datetime
from kafka import KafkaProducer
from scapy.utils import RawPcapReader
from scapy.layers.l2 import Ether
from scapy.layers.inet import IP, TCP, UDP

# ⚙️ Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

pcap_file = '/app/data/test1.pcap'
start_time = None
real_start = time.perf_counter()
sent_count = 0

def packet_to_json(pkt):
    ip_layer = pkt.getlayer(IP)
    tcp_layer = pkt.getlayer(TCP)
    udp_layer = pkt.getlayer(UDP)

    if not ip_layer or (not tcp_layer and not udp_layer):
        return None

    protocol = 6 if tcp_layer else 17

    def get_attr(layer, attr, default=0):
        try:
            return int(getattr(layer, attr, default))
        except:
            return default

    def get_flag(flag_str, char):
        try:
            return 1 if char in str(flag_str) else 0
        except:
            return 0

    # ✅ Tính TCP payload length
    if tcp_layer:
        try:
            ip_total_len = ip_layer.len
            ip_header_len = ip_layer.ihl * 4
            tcp_header_len = tcp_layer.dataofs * 4
            tcp_payload_len = max(0, ip_total_len - ip_header_len - tcp_header_len)
        except:
            tcp_payload_len = 0
    else:
        tcp_payload_len = 0

    # ✅ Tính UDP payload length (UDP header luôn 8 bytes)
    if udp_layer:
        udp_len = max(0, get_attr(udp_layer, 'len', 0) - 8)
    else:
        udp_len = 0

    return {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
        "src_ip": ip_layer.src,
        "dst_ip": ip_layer.dst,
        "length": len(pkt),
        "protocol": protocol,
        "src_port": get_attr(tcp_layer or udp_layer, 'sport', 0),
        "dst_port": get_attr(tcp_layer or udp_layer, 'dport', 0),
        "udp_len": udp_len,
        "tcp_seq": get_attr(tcp_layer, 'seq', 0),
        "tcp_ack": get_attr(tcp_layer, 'ack', 0),
        "tcp_win": get_attr(tcp_layer, 'window', 0),
        "tcp_len": tcp_payload_len,
        "cwr_flag": get_flag(tcp_layer.flags, 'C') if tcp_layer else 0,
        "ece_flag": get_flag(tcp_layer.flags, 'E') if tcp_layer else 0,
        "urg_flag": get_flag(tcp_layer.flags, 'U') if tcp_layer else 0,
        "ack_flag": get_flag(tcp_layer.flags, 'A') if tcp_layer else 0,
        "psh_flag": get_flag(tcp_layer.flags, 'P') if tcp_layer else 0,
        "rst_flag": get_flag(tcp_layer.flags, 'R') if tcp_layer else 0,
        "syn_flag": get_flag(tcp_layer.flags, 'S') if tcp_layer else 0,
        "fin_flag": get_flag(tcp_layer.flags, 'F') if tcp_layer else 0,
    }

# 🚀 Gửi dữ liệu từ pcap
print("🚀 Bắt đầu gửi gói tin từ pcap...")

for pkt_data, pkt_metadata in RawPcapReader(pcap_file):
    try:
        pkt_time = float(pkt_metadata.sec) + float(pkt_metadata.usec) / 1_000_000

        if start_time is None:
            start_time = pkt_time

        # ⏱ Mô phỏng thời gian thực
        delay = (pkt_time - start_time) - (time.perf_counter() - real_start)
        if delay > 0:
            time.sleep(delay)

        pkt = Ether(pkt_data)
        data = packet_to_json(pkt)

        if not data:
            continue

        producer.send('ddos_packets_raw', value=data)
        print(f"📤 Gửi lúc: {data['timestamp']}")
        sent_count += 1

    except Exception as e:
        print(f"❌ Bỏ qua gói lỗi: {e}")
        continue

producer.flush()
print(f"🎉 Đã gửi tổng cộng {sent_count} gói tin.")