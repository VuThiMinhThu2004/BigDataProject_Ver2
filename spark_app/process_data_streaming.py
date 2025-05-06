# features:
#     brand: String
#     price: Float
#     event_weekday: Int
#     category_code_1: String
#     category_code_2: String
#     activity_count: Int

import os
import json
import redis
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf, expr, split, to_timestamp, date_format, lit, dayofweek, when
from pyspark.sql.types import (
    StructType, StructField, StringType, ArrayType,
    LongType, DoubleType, TimestampType
)

from minio_config import config

# Important config variables
KAFKA_BOOTSTRAP_SERVERS = "broker:29092"
KAFKA_TOPIC = "data_streaming"
REDIS_HOST = "redis"
REDIS_PORT = 6379
MINIO_BUCKET = "bronze"
CHECKPOINT_LOCATION = f"s3a://{MINIO_BUCKET}/_checkpoints/data_streaming" # Lưu checkpoint trên MinIO

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("KafkaMinioRedisStreaming") \
    .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION) \
    .getOrCreate()

# Config access to MinIO
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "minio:9000")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", config['access_key'])
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", config['secret_key'])
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# Define schema for Minio notification events received from Kafka, only Key is enough to query precise file path
kafka_message_schema = StructType([
    StructField("Key", StringType(), True)
])

# Defne data schema for data stored as JSON format in bronze storage ~ Minio/bronze
minio_json_schema = StructType([
    StructField("event_timestamp", TimestampType(), True),
    StructField("user_id", LongType(), True),
    StructField("product_id", LongType(), True),
    StructField("user_session", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("brand", StringType(), True),
    StructField("category_code_level1", StringType(), True),
    StructField("category_code_level2", StringType(), True),
    StructField("event_weekday", LongType(), True), 
    StructField("activity_count", LongType(), True),
    StructField("is_purchased", LongType(), True),
])

def process_batch(batch_df, batch_id):
    """
    Hàm xử lý cho từng micro-batch trong Structured Streaming.
    Đọc file từ MinIO dựa trên key từ Kafka và lưu vào Redis.
    """
    print(f"--- Processing Batch ID: {batch_id} ---")
    if batch_df.isEmpty():
        print("Batch is empty.")
        return

    # batch_df chứa cột 'minio_object_key' được trích xuất từ Kafka message
    # Xây dựng đường dẫn S3 đầy đủ
    batch_df = batch_df.withColumn("s3_path", expr(f"concat('s3a://', minio_object_key)"))

    # Lấy danh sách các đường dẫn file cần đọc duy nhất trong batch này
    paths_to_read = [row.s3_path for row in batch_df.select("s3_path").distinct().collect()]

    if not paths_to_read:
        print("No paths to read in this batch.")
        return

    print(f"Reading files from MinIO: {paths_to_read}")

    try:
        # Đọc tất cả các file JSON tương ứng từ MinIO
        # Spark sẽ tự động phân tích JSON dựa vào schema
        minio_data_df = spark.read.schema(minio_json_schema).json(paths_to_read)

        print("MinIO Data Schema (Raw):")
        minio_data_df.printSchema()
        print("Sample MinIO Data (Raw):")
        minio_data_df.show(5, truncate=False)

        # ✨ Xử lý dữ liệu (nếu cần thêm)
        # Các trường category_code_level1, category_code_level2, event_weekday, activity_count đã có từ JSON
        # Giữ nguyên giá trị null, không thay thế
        processed_df = minio_data_df
            # Thêm các xử lý khác nếu cần

        print("Processed Data Schema:")
        processed_df.printSchema()
        print("Sample Processed Data:")
        processed_df.show(5, truncate=False)

        # --- Lưu dữ liệu vào Redis ---
        processed_df.foreachPartition(save_partition_to_redis)

    except Exception as e:
        print(f"Error reading from MinIO or writing to Redis for batch {batch_id}: {e}")
        # Có thể thêm logic xử lý lỗi ở đây (ví dụ: ghi log, gửi cảnh báo)

def save_partition_to_redis(partition_iterator):
    """
    Hàm được gọi cho mỗi partition của DataFrame để lưu dữ liệu vào Redis.
    Mở một kết nối Redis cho mỗi partition.
    """
    redis_client = None
    try:
        print(f"Connecting to Redis at {REDIS_HOST}:{REDIS_PORT}")
        redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True) # decode_responses=True để key/value là string
        redis_client.ping() # Kiểm tra kết nối
        print("Successfully connected to Redis for partition.")

        for row in partition_iterator:
            try:
                # Quyết định key cho Redis
                # Sử dụng user_id, product_id, user_session làm key
                redis_key = f"user:{row.user_id}:product:{row.product_id}:session:{row.user_session}"

                # Các trường cần lưu vào Redis Hash
                # Lấy các trường đã có từ JSON và xử lý
                redis_value = {
                    "brand": str(row.brand),
                    "price": str(row.price),
                    "event_weekday": str(row.event_weekday),
                    "category_code_1": str(row.category_code_level1),
                    "category_code_2": str(row.category_code_level2),
                    "activity_count": str(row.activity_count),
                    "is_purchased": str(row.is_purchased),
                    "user_session": str(row.user_session), # Thêm user_session nếu cần
                    "event_timestamp": str(row.event_timestamp) # Thêm timestamp nếu cần
                }

                # Lưu vào Redis Hash
                redis_client.hset(redis_key, mapping=redis_value)

                # Optional debug:
                # print(f"Saved to Redis: {redis_key} -> {redis_value}")

            except Exception as e:
                print(f"Error processing row or saving to Redis: {e}. Row: {row.asDict()}")
                # Ghi log lỗi chi tiết nếu cần

    except redis.exceptions.ConnectionError as e:
        print(f"FATAL: Could not connect to Redis for partition: {e}")
        # Nên có cơ chế retry hoặc báo lỗi nghiêm trọng ở đây
    except Exception as e:
        print(f"FATAL: Error in Redis saving partition: {e}")
    finally:
        if redis_client:
            print("Closing Redis connection for partition.")
            # Không cần close() vì StrictRedis quản lý connection pool
            pass

print("Spark Session Created. Reading from Kafka...")

# Read msg from Kafka Topic
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# Parse msg reeceived from Kafka Topic
# Chuyển value từ binary sang string và parse JSON
parsed_df = kafka_df \
    .select(col("value").cast("string").alias("kafka_value_str")) \
    .filter(col("kafka_value_str").isNotNull()) \
    .select(from_json(col("kafka_value_str"), kafka_message_schema).alias("kafka_data")) \
    .select(col("kafka_data.Key").alias("minio_object_key")) \
    .filter(col("minio_object_key").isNotNull()) \
    .filter(col("minio_object_key") != "") # Bỏ qua các message không có key

print("Kafka Schema Parsed. Starting foreachBatch processing...")

# --- Ghi stream sử dụng foreachBatch ---
query = parsed_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("update") \
    .trigger(processingTime="30 seconds") \
    .start()

print("Streaming query started. Waiting for termination...")
query.awaitTermination()