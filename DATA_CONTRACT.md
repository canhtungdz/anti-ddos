# DATA CONTRACT

Tài liệu mô tả cấu trúc dữ liệu chuẩn trong file JSON được sử dụng trong dự án.

---

```jsonc
// UDP Packet (protocol = 17)
{
  "timestamp": "0.000000000",      // (str) Thời điểm gói tin được ghi lại
  "src_ip": "172.16.0.5",          // (str) Địa chỉ IP nguồn
  "dst_ip": "192.168.50.1",        // (str) Địa chỉ IP đích
  "length": 558,                   // (int) Tổng chiều dài gói tin (bytes)
  "protocol": 17,                  // (int) Giao thức (17 = UDP, 6 = TCP)
  "src_port": 62466,               // (int) Cổng nguồn
  "dst_port": 58306,               // (int) Cổng đích
  "udp_len": 524,                  // (int) Độ dài phần UDP payload

  // Dưới đây là các trường TCP (nếu có)
  "tcp_seq": 5243,                 // (int) Số thứ tự TCP (Sequence Number)
  "tcp_ack": 2084,                 // (int) Số xác nhận TCP (Acknowledgment Number)
  "tcp_win": 259,                  // (int) **Kích thước cửa sổ nhận (TCP Window Size)**.
                                   //       Đây là **số byte dữ liệu mà máy nhận sẵn sàng nhận tiếp theo**.
                                   //       Nó giúp kiểm soát luồng và đảm bảo không bị quá tải khi truyền dữ liệu.
  "tcp_len": 879,                  // (int) Độ dài dữ liệu TCP payload (không tính header)
  
  // Các cờ TCP flags (bit)
  "cwr_flag": 0,                   // (int) Cờ CWR (Congestion Window Reduced)
  "ece_flag": 0,                   // (int) Cờ ECE (Explicit Congestion Notification Echo)
  "urg_flag": 0,                   // (int) Cờ URG (Urgent)
  "ack_flag": 1,                   // (int) Cờ ACK (Acknowledgment)
  "psh_flag": 1,                   // (int) Cờ PSH (Push)
  "rst_flag": 0,                   // (int) Cờ RST (Reset)
  "syn_flag": 0,                   // (int) Cờ SYN (Synchronize)
  "fin_flag": 0                    // (int) Cờ FIN (Finish)
}

