import json
import time
import os
from datetime import datetime
from scapy.utils import RawPcapReader
from scapy.layers.l2 import Ether
from scapy.layers.inet import IP, TCP, UDP

# ⚙️ Configuration
pcap_file = '/Users/nguyencanhtung/Program/anti-ddos/data/SAT-01-12-2018_0817.pcap'
output_dir = '/Users/nguyencanhtung/Program/anti-ddos/data/stream_input'  # Thư mục output cho Spark
batch_size = 100  # Số packet per file
start_time = None
real_start = time.perf_counter()
sent_count = 0
batch_count = 0
current_batch = []

# Tạo thư mục output nếu chưa có
os.makedirs(output_dir, exist_ok=True)

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
        "timestamp": datetime.now().isoformat() + "+00:00",  # ISO format với timezone
        "src_ip": str(ip_layer.src),
        "dst_ip": str(ip_layer.dst),
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

def save_batch_to_file(batch_data, batch_num):
    """Lưu batch data thành file JSON Lines"""
    filename = f"batch_{batch_num:04d}.json"
    filepath = os.path.join(output_dir, filename)
    
    # ✅ Lưu dạng JSON Lines (mỗi dòng 1 JSON object)
    with open(filepath, 'w') as f:
        for record in batch_data:
            f.write(json.dumps(record) + '\n')
    
    print(f"💾 Saved {len(batch_data)} records to {filename}")

def save_batch_to_file_array(batch_data, batch_num):
    """Lưu batch data thành file JSON Array (nếu muốn format [{},{}])"""
    filename = f"batch_array_{batch_num:04d}.json"
    filepath = os.path.join(output_dir, filename)
    
    # ✅ Lưu dạng JSON Array
    with open(filepath, 'w') as f:
        json.dump(batch_data, f, indent=2)
    
    print(f"💾 Saved {len(batch_data)} records to {filename} (Array format)")

# 🚀 Xử lý dữ liệu từ pcap
print(f"🚀 Bắt đầu đọc gói tin từ {pcap_file}...")
print(f"📁 Output directory: {output_dir}")
print(f"📦 Batch size: {batch_size} packets per file")

for pkt_data, pkt_metadata in RawPcapReader(pcap_file):
    try:
        pkt_time = float(pkt_metadata.sec) + float(pkt_metadata.usec) / 1_000_000

        if start_time is None:
            start_time = pkt_time

        # ⏱ Mô phỏng thời gian thực (tùy chọn - có thể bỏ để xử lý nhanh hơn)
        # delay = (pkt_time - start_time) - (time.perf_counter() - real_start)
        # if delay > 0:
        #     time.sleep(delay)

        pkt = Ether(pkt_data)
        data = packet_to_json(pkt)

        if not data:
            continue

        # Thêm vào batch hiện tại
        current_batch.append(data)
        sent_count += 1

        # Nếu batch đầy, lưu file
        if len(current_batch) >= batch_size:
            save_batch_to_file(current_batch, batch_count)
            
            # ✅ Tùy chọn: Lưu thêm file JSON Array format
            # save_batch_to_file_array(current_batch, batch_count)
            
            current_batch = []
            batch_count += 1
            
            # ✅ Delay giữa các batch để simulate real-time stream
            time.sleep(1)  # 1 giây delay giữa các batch

        if sent_count % 100 == 0:
            print(f"📊 Processed {sent_count} packets...")

    except Exception as e:
        print(f"❌ Bỏ qua gói lỗi: {e}")
        continue

# ✅ Lưu batch cuối cùng (nếu có)
if current_batch:
    save_batch_to_file(current_batch, batch_count)
    batch_count += 1

print(f"🎉 Hoàn thành!")
print(f"📊 Total packets processed: {sent_count}")
print(f"📁 Total files created: {batch_count}")
print(f"📂 Files saved in: {output_dir}")

# ✅ Liệt kê các file đã tạo
print("\n📄 Created files:")
for filename in sorted(os.listdir(output_dir)):
    if filename.endswith('.json'):
        filepath = os.path.join(output_dir, filename)
        file_size = os.path.getsize(filepath)
        print(f"  {filename} ({file_size:,} bytes)")
