# DDoS Detection using Spark MLlib (Random Forest Model)

## 📘 Mục tiêu

Ứng dụng phát hiện các luồng dữ liệu DDoS theo thời gian thực (real-time) bằng cách sử dụng mô hình Random Forest được huấn luyện trên Spark MLlib. Dữ liệu đầu vào đến từ Kafka và đầu ra là nhãn phân loại: `Normal` hoặc `DDoS`.

---

## Yêu cầu hệ thống

* Apache Spark 3.x (hỗ trợ Structured Streaming)
* Kafka topic chứa dữ liệu real-time (ví dụ: `ddos_packets_raw`)
* Mô hình huấn luyện sẵn (`rf_binary_model`)
* File đặc trưng yêu cầu (`expected_features.txt`)
* Docker Compose (nếu chạy theo dạng cụm Spark + Kafka)

---

## Cấu trúc thư mục

```
spark_app/
├── ml_model/
│   ├── rf_binary_model/               #  Mô hình Random Forest đã huấn luyện
│   ├── expected_features.txt          #  Danh sách các cột đặc trưng
│   └── README.md                      #  Tài liệu mô tả mô hình
├── predict_rf_spark.py               #  Dự đoán batch trên file CSV
├── train_rf.py                       # Script huấn luyện mô hình Random Forest
├── main.py                           # (tuỳ chọn) chạy chính
└── README.md                         #  Hướng dẫn tổng thể
```

---

## Cách huấn luyện mô hình (1 lần duy nhất)

> Nếu bạn chưa có mô hình, hãy chạy script sau để train:

```bash
docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-apps/train_rf.py
```

Sau khi chạy xong, thư mục `/opt/ml-model/rf_binary_model` sẽ chứa pipeline đã huấn luyện. File `/opt/ml-model/expected_features.txt` cũng được tạo kèm.

---

## Cách chạy dự đoán real-time

### 1. Kafka phải gửi dữ liệu lên topic (ví dụ `ddos_packets_raw`), mỗi bản ghi là 1 JSON object với các đặc trưng định lượng (numeric features).

Ví dụ một bản ghi:

```json
{
  "Unnamed_0": 1,
  "Source_IP": "192.168.1.1",
  "Destination_IP": "10.0.0.5",
  "Timestamp": "2025-07-10T15:00:00Z",
  "SimillarHTTP": 0,
  "Inbound": 1,
  "Flow_Duration": 12000,
  "Total_Fwd_Packets": 10,
  "Total_Backward_Packets": 5
  // ... các đặc trưng khác
}
```

> ⚠️ Bạn cần xử lý đầu vào để loại bỏ các cột không nằm trong danh sách `expected_features.txt`, chẳng hạn như: `Unnamed_0`, `Source_IP`, `Destination_IP`, `Timestamp`, `SimillarHTTP`, `Inbound`...

---

## 🔧 Làm sạch dữ liệu đầu vào

Trước khi đưa vào mô hình, cần đảm bảo dữ liệu:

* Làm sạch tên trường dữ liệu
* Chỉ bao gồm các cột trong `expected_features.txt`
* Không chứa giá trị `null`, `NaN`, `inf`, `-inf`
* Tất cả giá trị là kiểu số (`float` hoặc `double`)

Ví dụ đoạn code làm sạch:

```python
from pyspark.sql.functions import col, when

# ✂️ Làm sạch tên cột
df = df.toDF(*[c.strip() for c in df.columns])
for old_name in df.columns:
    new_name = old_name.replace(" ", "_").replace(".", "_")
    if old_name != new_name:
        df = df.withColumnRenamed(old_name, new_name)

# Loại bỏ các cột không nằm trong expected_features.txt
df = df.select(*expected_features)

# Thay thế Inf / -Inf bằng null
for c in df.columns:
    df = df.withColumn(c, when(col(c).isin(float("inf"), float("-inf")), None).otherwise(col(c)))

# Loại bỏ dòng null
df = df.dropna()
```

---

## 🧪 Viết script chạy mô hình real-time (`predict_rf.py`)

Bạn có thể viết script dự đoán theo thời gian thực như sau:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when
from pyspark.sql.types import StructType, DoubleType
from pyspark.ml import PipelineModel

# Tạo Spark session với Structured Streaming
spark = SparkSession.builder \
    .appName("Real-time DDoS Prediction") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Đọc danh sách cột từ file expected_features.txt
with open("/opt/ml-model/expected_features.txt") as f:
    feature_cols = [line.strip() for line in f if line.strip()]

# Tạo schema cho JSON đầu vào
schema = StructType()
for col_name in feature_cols:
    schema = schema.add(col_name, DoubleType())

# Đọc luồng từ Kafka
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "ddos_packets_raw") \
    .load()

# Parse JSON và chọn các cột đúng thứ tự
parsed_df = raw_df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# Làm sạch dữ liệu (nếu cần)
for c in feature_cols:
    parsed_df = parsed_df.withColumn(c, when(col(c).isin(float("inf"), float("-inf")), None).otherwise(col(c)))
parsed_df = parsed_df.dropna()

# Tải mô hình đã huấn luyện
model = PipelineModel.load("/opt/ml-model/rf_binary_model")

# Dự đoán
predictions = model.transform(parsed_df)

# Chuyển kết quả ra nhãn (0.0 → Normal, 1.0 → DDoS)
result = predictions.withColumn("Label", 
            when(col("prediction") == 1.0, "DDoS").otherwise("Normal")).select("Label")

# Ghi ra console
query = result.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
```

---

## 🚀 Chạy script trong container

```bash
docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-apps/predict_rf.py
```

---

## 💥 Đầu ra

Dự đoán xuất hiện trong console dưới dạng:

```
+----------+
| Label    |
+----------+
| DDoS     |
| Normal   |
+----------+
```

