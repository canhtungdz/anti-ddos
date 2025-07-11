from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, trim, upper
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import os

# 🚀 Tạo Spark session
spark = SparkSession.builder \
    .appName("Train Binary Random Forest for DDoS") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 📥 Đọc tất cả file .csv
df = spark.read.csv("/opt/spark-data/*.csv", header=True, inferSchema=True)

# ✂️ Làm sạch tên cột
df = df.toDF(*[c.strip() for c in df.columns])
for old_name in df.columns:
    new_name = old_name.replace(" ", "_").replace(".", "_")
    if old_name != new_name:
        df = df.withColumnRenamed(old_name, new_name)

# 🧹 Xoá cột không cần thiết
cols_to_drop = ['Unnamed:_0', 'Flow_ID', 'Source_IP', 'Destination_IP', 'Timestamp', 'SimillarHTTP', 'Inbound']
df = df.drop(*[col_name for col_name in cols_to_drop if col_name in df.columns])

# 🔤 Làm sạch nhãn (Label)
df = df.withColumn("Label_cleaned", upper(trim(col("Label"))))

# 🔁 Chuyển nhãn sang binary: BENIGN → 0.0, còn lại → 1.0
df = df.withColumn("binary_label", when(col("Label_cleaned") == "BENIGN", 0.0).otherwise(1.0))

# 🧽 Thay thế Inf và NaN
for c in df.columns:
    if c not in ["binary_label", "Label", "Label_cleaned"]:
        df = df.withColumn(c, when(col(c).isin(float("inf"), float("-inf")), None).otherwise(col(c)))

df = df.dropna()

# 🛑 Kiểm tra rỗng
if df.count() == 0:
    raise ValueError("❌ Dữ liệu rỗng sau khi làm sạch!")

# 📊 Thống kê phân phối nhãn
print("📊 Phân phối nhãn sau xử lý:")
df.groupBy("binary_label").count().orderBy("binary_label").show()

# 🧮 Vector hóa đặc trưng
feature_cols = [c for c in df.columns if c not in ["Label", "Label_cleaned", "binary_label"]]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

# 🌲 Random Forest phân loại nhị phân
rf = RandomForestClassifier(
    labelCol="binary_label",
    featuresCol="features",
    numTrees=100,
    maxDepth=6,
    impurity="gini",
    featureSubsetStrategy="sqrt"
)

# 🔗 Pipeline
pipeline = Pipeline(stages=[assembler, rf])

# ✂️ Chia train/test
train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)

# 🧠 Huấn luyện
print("🚀 Đang huấn luyện mô hình Random Forest có cân bằng nhãn...")
rf_model = pipeline.fit(train_data)

# 💾 Lưu mô hình
model_path = "/opt/ml-model/rf_binary_model"
rf_model.write().overwrite().save(model_path)
print(f"✅ Mô hình đã lưu tại: {model_path}")

# ✍️ Ghi danh sách đặc trưng ra file
features_txt_path = "/opt/ml-model/expected_features.txt"
os.makedirs("/opt/ml-model", exist_ok=True)
with open(features_txt_path, "w") as f:
    for col_name in feature_cols:
        f.write(col_name + "\n")
print(f"✅ Đã lưu danh sách đặc trưng vào: {features_txt_path}")

# 🔍 Dự đoán
predictions = rf_model.transform(test_data)

# 🧾 Ma trận nhầm lẫn
print("\n📉 Ma trận nhầm lẫn:")
predictions.groupBy("binary_label", "prediction").count().orderBy("binary_label", "prediction").show()

# 🎯 Đánh giá
evaluator = MulticlassClassificationEvaluator(labelCol="binary_label", predictionCol="prediction")
accuracy = evaluator.setMetricName("accuracy").evaluate(predictions)
precision = evaluator.setMetricName("weightedPrecision").evaluate(predictions)
recall = evaluator.setMetricName("weightedRecall").evaluate(predictions)
f1 = evaluator.setMetricName("f1").evaluate(predictions)

# 📈 In kết quả
print("\n📈 KẾT QUẢ ĐÁNH GIÁ MÔ HÌNH:")
print(f"🎯 Accuracy        : {accuracy * 100:.2f}%")
print(f"📌 Precision       : {precision * 100:.2f}%")
print(f"📥 Recall          : {recall * 100:.2f}%")
print(f"🏆 F1 Score        : {f1 * 100:.2f}%")
