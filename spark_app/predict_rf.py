from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.ml import PipelineModel
import pandas as pd
import matplotlib.pyplot as plt
import os

# 🚀 1. Khởi tạo Spark session
spark = SparkSession.builder.appName("Predict with RF Binary Model").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 📥 2. Đọc dữ liệu test
df = spark.read.csv("/opt/spark-data/MSSQL_first_100k.csv", header=True, inferSchema=True)
df = df.toDF(*[c.strip() for c in df.columns])  # Bỏ khoảng trắng tên cột

# 🧹 3. Làm sạch dữ liệu
# ✅ Chuẩn hóa tên cột
for old_name in df.columns:
    new_name = old_name.replace(" ", "_").replace(".", "_")
    if old_name != new_name:
        df = df.withColumnRenamed(old_name, new_name)

# ✅ Xoá cột không cần thiết
cols_to_drop = ['Unnamed_0', 'Source_IP', 'Destination_IP', 'Timestamp', 'SimillarHTTP', 'Inbound']
df = df.drop(*[col for col in cols_to_drop if col in df.columns])

# ✅ Thay thế inf/-inf bằng null
for c in df.columns:
    df = df.withColumn(c, when(col(c).isin(float('inf'), float('-inf')), None).otherwise(col(c)))

# ✅ Xoá dòng null
df = df.dropna()

# 📦 4. Tải mô hình đã huấn luyện
model_path = "/opt/ml-model/rf_binary_model"
rf_model = PipelineModel.load(model_path)

# 🔮 5. Dự đoán
predictions = rf_model.transform(df)

# 🐼 6. Chuyển sang Pandas để xử lý kết quả
predictions_pd = predictions.toPandas()

# ✅ 7. Thêm cột nhãn dự đoán
predictions_pd["Predicted_Label"] = predictions_pd["prediction"].map({0.0: "Normal", 1.0: "DDoS"})

# ✅ 8. Loại bỏ cột prediction nếu không cần, giữ lại tất cả đặc trưng và thêm cột Predicted_Label
final_result = predictions_pd.drop(columns=["prediction"])
out_path = "/opt/spark-output/prediction_result_rf_full.csv"
os.makedirs("/opt/spark-output", exist_ok=True)
final_result.to_csv(out_path, index=False)

# ✅ 9. Thông báo hoàn tất
print(f"✅ Đã lưu kết quả tại: {out_path}")

# 📊 10. Vẽ biểu đồ phân bố nhãn (không show GUI)
final_result["Predicted_Label"].value_counts().sort_index().plot(kind='bar', color='lightblue')
plt.xlabel("Nhãn dự đoán")
plt.ylabel("Số lượng Flow")
plt.title("Phân bố nhãn dự đoán (RF)")
plt.grid(True)
plt.tight_layout()
plt.savefig("/opt/spark-output/predicted_label_rf_full.png")
plt.close()  # ✅ Không dùng plt.show() để tránh lỗi trong môi trường không hỗ trợ GUI
