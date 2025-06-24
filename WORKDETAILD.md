## Cấu trúc thư mục
File **DATA_CONTRACT.md** chứa cấu trúc data để làm việc (data dạng JSON)
**docker-compose.yml** là file cấu hình docker-compose
**README.md** chứa mô tả của dự án
**WORKDETAILD.md** chứa mô tả dự án cho thành viên team.
**data** chứa cách tải data và data (lưu ý: không push data lên github).

Tầng 1 (mô phỏng và kafka) tại thư mục kafka_producer
Tầng 2 (xử lý với spark) tại thư mục spark_app
## Cách sử dụng
### Tải các dịch vụ cần thiết với docker-compose.yml
Di chuyển đến thư mục gốc của dự án (anti-ddos) tại terminal (command promt).
**docker-compose up -d** Tiến hành tải các image kafka, spark, zookeeper đã được cấu hình sẵn tại file docker-compose.yml
**docker-compose ps** Kiểm tra các container
**docker-compose stop** Tạm dừng các container
**docker-compose start** Tiếp tục chạy các container
**docker-compose down** Dừng và xoá các container

### Làm việc với github
1. Clone the repository
git clone https://github.com/canhtungdz/anti-ddos.git
cd anti-ddos
2. Tạo một nhánh mới
tiến hành tạo nhánh ứng với chức năng đang làm việc.
git checkout -b feature-name
3. Lưu thay đổi và commit
git add .
git commit -m "Add description of changes"
4. Đẩy lên github
git push -u origin feature-name
5. Tạo Pull Request
Đến repo của project.
Ấn **Compare & pull request**, viết mô tả, và gửi **Create pull request**
