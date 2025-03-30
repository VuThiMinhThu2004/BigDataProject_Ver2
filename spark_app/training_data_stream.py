from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf, expr
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, TimestampType, DoubleType
from minio import Minio
import urllib.parse
import logging

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from minio_config import config

# Config Spark to directly access MinIO
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "minio:9000")
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", config['access_key'])
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", config['secret_key'])
spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
spark._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("KafkaExample") \
    .config("spark.streaming.stopGracefullyonShutdown", True)\
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0")\
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4") \
    .config("spark.sql.shuffle.partitions", 4)\
    .master("spark://spark-master:7077")\
    .getOrCreate()

# Connect Kafka to Spark, then it allows to read messages from specific Kafka topics 
kafkaDF = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", "training_data_streaming") \
    .option('startingOffsets', 'earliest') \
    .load()

# Define schema for Minio notification events received from Kafka  
minio_event_schema = StructType([
    StructField("EventName", StringType(), True),
    StructField("Key", StringType(), True),
    StructField("Records", ArrayType(
        StructType([
            StructField("eventName", StringType(), True),
            StructField("s3", StructType([
                StructField("bucket", StructType([
                    StructField("name", StringType(), True)
                ]), True),
                StructField("object", StructType([
                    StructField("key", StringType(), True),
                    StructField("size", StringType(), True),
                    StructField("contentType", StringType(), True)
                ]), True)
            ]), True)
        ])
    ), True)  
])

# Define schema for e-commerce data
ecommerce_schema = StructType([
    StructField("event_time", TimestampType(), True),    # Thời gian xảy ra sự kiện (UTC)
    StructField("event_type", StringType(), True),       # Loại sự kiện: view, cart, remove_from_cart, purchase
    StructField("product_id", StringType(), True),      # ID sản phẩm
    StructField("category_id", StringType(), True),     # ID danh mục sản phẩm
    StructField("category_code", StringType(), True),    # Tên danh mục (có thể null)
    StructField("brand", StringType(), True),            # Tên thương hiệu (có thể null)
    StructField("price", DoubleType(), True),            # Giá sản phẩm
    StructField("user_id", StringType(), True),         # ID người dùng
    StructField("user_session", StringType(), True)      # ID phiên làm việc của người dùng
])

# Parse event received from Kafka
parsed_events = kafkaDF.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), minio_event_schema).alias('event')).select("event.*")

   
def write_to_postgres(batch_df, batch_id):
    try:
        batch_df.write \
            .format("jdbc") \
            .mode("append") \
            .option("url", "jdbc:postgresql://postgres:5432/airflow") \
            .option("dbtable", "processed_data") \
            .option("user", "airflow") \
            .option("password", "airflow") \
            .option("driver", "org.postgresql.Driver")\
            .option("createTableColumnTypes", """
                event_time TIMESTAMP, 
                event_type VARCHAR(50), 
                product_id VARCHAR(100), 
                category_id VARCHAR(100), 
                category_code VARCHAR(255), 
                brand VARCHAR(255), 
                price DECIMAL(10,2),
                user_id VARCHAR(100),
                user_session VARCHAR(255)
            """) \
            .save()
        logging.info(f"Batch {batch_id} loaded successfully")
    except Exception as e:
        logging.error(f"Error writing batch {batch_id}: {str(e)}")
        
def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        logging.info(f"Batch {batch_id} is empty")
        return
    try:
        # Extract bucket name & object key from event notification messages received from Kafka
        keys_df = batch_df.filter(col("Records").isNotNull()) \
            .select(
                col("Records")[0]["s3"]["bucket"]["name"].alias("bucket_name"),
                col("Records")[0]["s3"]["object"]["key"].alias("object_key")
            )
        
        if keys_df.isEmpty():
            logging.info(f"No valid MinIO events in batch {batch_id}")
            return   
        
        # Xử lý từng hàng (mỗi thông báo sự kiện)
        for row in keys_df.collect():
            bucket_name = row["bucket_name"]
            encoded_object_key = row["object_key"]
            
            # URL decode key
            object_key = urllib.parse.unquote(encoded_object_key)
            
            logging.info(f"Processing file: {object_key} from bucket: {bucket_name}")
            
            try:
                # Đọc file Parquet từ MinIO
                response = minio_client.get_object(bucket_name, object_key)
                parquet_data = response.read()
                
                # Ghi tạm ra file để Spark đọc (Spark cần đường dẫn file)
                temp_path = f"/tmp/parquet_file_{batch_id}.parquet"
                with open(temp_path, "wb") as f:
                    f.write(parquet_data)
                
                # Đọc Parquet bằng Spark
                user_df = spark.read.parquet(temp_path)
                
                # Ghi dữ liệu vào PostgreSQL
                write_to_postgres(user_df, f"{batch_id}_{object_key}")
            
            except Exception as e:
                logging(f"Error processing {object_key}: {e}")
    except Exception as e:
        logging.error(f"Error in batch {batch_id}: {str(e)}")
                
query = parsed_events.writeStream \
                    .foreachBatch(process_batch) \
                        .outputMode("append") \
                            .start()\
                                .awaitTermination()