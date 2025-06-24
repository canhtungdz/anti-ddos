## Tên project: anti-ddos
## Nội dung: Giả lập cuộc tấn công DDoS, thu thập, xử lý và phát hiện truy cập độc hại.

## Cách sử dụng


## Thành phần project:
### Tầng 1: Mô phỏng
Đọc dữ liệu từ bộ dữ liệu (file PCAP), mô phỏng cuộc tấn công. Chuyển dữ liệu sang dạng JSON. Tiến hành dùng kafka (với thư viện kafka-python) để lấy dữ liệu.
Phần này sẽ ở thư mục kafka_producer.

### Tầng 2: Xử lý
Làm việc với spark, nhận dữ liệu từ tầng 1 (kafka) tiến hành xử lý, tính toán, lưu trữ.
Làm việc trong thư mục spark_app

### Tầng 3: Phát hiện
Phát hiện truy cập đọc hại.
Làm việc trong thư mục ml_model

### Tầng 4: Trực quan hoá
Tiến hành trực quan hoá project.