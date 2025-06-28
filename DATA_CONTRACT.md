# DATA CONTRACT

Tài liệu mô tả cấu trúc dữ liệu chuẩn trong file JSON được sử dụng trong dự án.

---

```jsonc
// UDP Packet (protocol = 17)
{
  "timestamp": "0.000000000",      // Thời điểm gói tin được ghi lại
  "src_ip": "172.16.0.5",          // Địa chỉ IP nguồn
  "dst_ip": "192.168.50.1",        // Địa chỉ IP đích
  "length": 558,                   // Tổng chiều dài gói tin (bytes)
  "protocol": 17,                  // Giao thức (17 = UDP)
  "src_port": 62466,               // Cổng nguồn
  "dst_port": 58306,               // Cổng đích
  "udp_len": 524                   // Độ dài phần UDP payload
}

// TCP Packet (protocol = 6)
{
  "timestamp": "16.340476000",     // Thời điểm gói tin được ghi lại
  "src_ip": "204.154.111.134",     // Địa chỉ IP nguồn
  "dst_ip": "192.168.50.6",        // Địa chỉ IP đích
  "length": 933,                   // Tổng chiều dài gói tin (bytes)
  "protocol": 6,                   // Giao thức (6 = TCP)
  "src_port": 443,                 // Cổng nguồn
  "dst_port": 58391,               // Cổng đích
  "tcp_seq": 5243,                 // Số thứ tự TCP (Sequence Number)
  "tcp_ack": 2084,                 // Số xác nhận TCP (Acknowledgment Number)
  "tcp_len": 879,                  // Độ dài dữ liệu TCP payload
  "cwr_flag": 0,                   // Cờ CWR (Congestion Window Reduced)
  "ece_flag": 0,                   // Cờ ECE (Explicit Congestion Notification Echo)
  "urg_flag": 0,                   // Cờ URG (Urgent)
  "ack_flag": 1,                   // Cờ ACK (Acknowledgment)
  "psh_flag": 1,                   // Cờ PSH (Push)
  "rst_flag": 0,                   // Cờ RST (Reset)
  "syn_flag": 0,                   // Cờ SYN (Synchronize)
  "fin_flag": 0                    // Cờ FIN (Finish)
}
