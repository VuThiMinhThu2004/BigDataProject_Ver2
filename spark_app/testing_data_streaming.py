import os
import json
import redis
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf, expr
from pyspark.sql.types import (
    StructType, StructField, StringType, ArrayType,
    LongType, DoubleType, TimestampType
)

from minio_config import config

# Important config variables
KAFKA_BOOTSTRAP_SERVERS = "broker:29092"  # Sử dụng listener nội bộ của Kafka
KAFKA_TOPIC = "testing_data_streaming"
REDIS_HOST = "redis"
REDIS_PORT = 6379
MINIO_BUCKET = "bronze"
CHECKPOINT_LOCATION = f"s3a://{MINIO_BUCKET}/_checkpoints/testing_data_streaming" # Lưu checkpoint trên MinIO

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
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# Define schema for Minio notification events received from Kafka, only Key is enough to query precise file path
kafka_message_schema = StructType([
    StructField("Key", StringType(), True)
    # Nếu cần đọc cấu trúc đầy đủ:
    # StructField("Records", ArrayType(StructType([
    #     StructField("s3", StructType([
    #         StructField("object", StructType([
    #             StructField("key", StringType(), True) # Key đã URL encoded
    #         ]), True)
    #     ]), True)
    # ])), True)
])

# Defne data schema for data stored as JSON format in bronze storage ~ Minio/bronze
minio_json_schema = StructType([
    StructField("event_time", StringType(), True), # Remain the StringType, or can change to TimestampType
    StructField("event_type", StringType(), True),
    StructField("product_id", LongType(), True),
    StructField("category_id", LongType(), True),
    StructField("category_code", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("user_id", LongType(), True),
    StructField("user_session", StringType(), True)
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
    batch_df = batch_df.withColumn("s3_path", expr(f"concat('s3a://{MINIO_BUCKET}/', minio_object_key)"))

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

        print("MinIO Data Schema:")
        minio_data_df.printSchema()
        print("Sample MinIO Data:")
        minio_data_df.show(5, truncate=False)

        # --- Lưu dữ liệu vào Redis ---
        minio_data_df.foreachPartition(save_partition_to_redis)

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
        redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=0)
        redis_client.ping() # Kiểm tra kết nối
        print("Successfully connected to Redis for partition.")

        for row in partition_iterator:
            try:
                # Quyết định key cho Redis
                # Ví dụ: sử dụng product_id làm key chính, hoặc kết hợp nhiều field
                # redis_key = f"product:{row.product_id}"
                # Ví dụ: sử dụng user_session và product_id
                redis_key = f"session:{row.user_session}:product:{row.product_id}"

                # Quyết định value cho Redis
                # Cách 1: Lưu toàn bộ row thành JSON string
                # redis_value = json.dumps(row.asDict())
                # redis_client.set(redis_key, redis_value)

                # Cách 2: Lưu thành Redis Hash (linh hoạt hơn)
                # Chuyển đổi row thành dict, đảm bảo các giá trị là string
                row_dict = {k: str(v) if v is not None else "" for k, v in row.asDict().items()}
                redis_client.hset(redis_key, mapping=row_dict)

                # print(f"Saved to Redis: Key='{redis_key}'")

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
            redis_client.close()



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

# Nếu cần decode key từ s3.object.key (URL encoded)
# from urllib.parse import unquote
# def url_decode(encoded_str):
#     if encoded_str:
#         return unquote(encoded_str)
#     return None
# url_decode_udf = udf(url_decode, StringType())
# parsed_df = kafka_df \
#     .select(col("value").cast("string").alias("kafka_value_str")) \
#     .filter(col("kafka_value_str").isNotNull()) \
#     .select(from_json(col("kafka_value_str"), kafka_message_schema).alias("kafka_data")) \
#     .select(col("kafka_data.Records")[0].s3.object.key.alias("encoded_key")) \
#     .withColumn("minio_object_key", url_decode_udf(col("encoded_key"))) \
#     .filter(col("minio_object_key").isNotNull())

print("Kafka Schema Parsed. Starting foreachBatch processing...")

# --- Ghi stream sử dụng foreachBatch ---
query = parsed_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("update") \
    .trigger(processingTime="30 seconds") \
    .start()

print("Streaming query started. Waiting for termination...")
query.awaitTermination()