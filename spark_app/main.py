from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, IntegerType

if __name__ == "__main__":
    print("Bắt đầu ứng dụng Spark Test...")

    # 1. Khởi tạo SparkSession, kết nối tới Spark Master trong Docker
    # Tên app là 'SparkTestApp'
    spark = SparkSession.builder \
        .appName("SparkTestApp") \
        .master("spark://spark-master:7077") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "ddos_packets_raw") \
        .load()

    # Chuyển cột value sang string để thao tác dễ dàng
    df_str = df.selectExpr("CAST(value AS STRING) as value_str")

    # Định nghĩa schema cho dữ liệu JSON
    schema = StructType() \
        .add("timestamp", StringType()) \
        .add("src_ip", StringType()) \
        .add("dst_ip", StringType()) \
        .add("length", IntegerType()) \
        .add("protocol", IntegerType()) \
        .add("src_port", IntegerType()) \
        .add("dst_port", IntegerType()) \
        .add("udp_len", IntegerType()) \
        .add("tcp_seq", IntegerType()) \
        .add("tcp_ack", IntegerType()) \
        .add("tcp_len", IntegerType()) \
        .add("cwr_flag", IntegerType()) \
        .add("ece_flag", IntegerType()) \
        .add("urg_flag", IntegerType()) \
        .add("ack_flag", IntegerType()) \
        .add("psh_flag", IntegerType()) \
        .add("rst_flag", IntegerType()) \
        .add("syn_flag", IntegerType()) \
        .add("fin_flag", IntegerType())

    # Parse JSON sang DataFrame với các cột
    df_parsed = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    # Ví dụ: tính tổng số gói tin theo src_ip
    feature_df = df_parsed.groupBy("src_ip").count()

    query = feature_df.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", False) \
        .start()

    query.awaitTermination()

