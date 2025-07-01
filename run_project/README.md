## Cách chạy project
# Lần đầu cần chạy lệnh build: **docker-compose build**

# Chạy lệnh up: **docker-compose up -d**
Trong đó đã mount các thư mục **data**, **producer**, **spark_app** vào trong thư mục /app của docker.
# Chạy lệnh **docker-compose exec run_project bash** để vào bash của docker.
Trong đó sẽ chạy được các file như trên máy chính.
Ví dụ **python /app/producer/sender/producer.py**

# Khi sửa các file tại các thư mục đã mount thì không cần build, up lại.