from pyspark.sql import SparkSession

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

    # 2. In schema của DataFrame
    # In schema sẽ hiển thị cấu trúc của dữ liệu nhận được từ Kafka
    df.printSchema()
    # df.show(truncate=False)

    query = df.writeStream \
        .format("console") \
        .option("truncate", False) \
        .start()

    query.awaitTermination()