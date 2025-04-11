from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf, expr, split, when
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, TimestampType, DoubleType, LongType, IntegerType
from minio import Minio
import urllib.parse
import logging
from pyspark.sql import Window
from pyspark.sql import functions as F
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from loguru import logger
import pandas as pd

import sys
import os

# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append('/opt/spark_app')

from minio_config import config

# # Initialize Spark Session
# spark = SparkSession.builder \
#     .appName("KafkaExample") \
#     .config("spark.streaming.stopGracefullyonShutdown", True)\
#     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0")\
#     .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4") \
#     .config("spark.sql.shuffle.partitions", 4)\
#     .master("spark://spark-master:7077")\
#     .getOrCreate()

# Initialize Spark Session không nhận event từ Kafka

spark = SparkSession.builder \
    .appName("LocalDataProcessing") \
    .config("spark.streaming.stopGracefullyonShutdown", True)\
    .config("spark.jars", "/opt/spark/jars/postgresql-42.7.5.jar") \
    .config("spark.sql.shuffle.partitions", 4) \
    .master("spark://spark-master:7077")\
    .getOrCreate()
    
# # Chưa dùng đến vì đang lấy parquet tĩnh để kiểm thử
# # Config Spark to directly access MinIO
# spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "minio:9000")
# spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", config['access_key'])
# spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", config['secret_key'])
# spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
# spark._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")

class PostgreSQLOfflineWriter:
    """Helper class to write batches of data to PostgreSQL offline store."""

    def __init__(
        self,
        host: str = None,
        port: int = None,
        database: str = None,
        db_schema: str = None,
        user: str = None,
        password: str = None,
        **kwargs,
    ):
        """Initialize PostgreSQL connection parameters."""
        self.host = host or os.getenv("POSTGRES_HOST", "localhost")
        self.port = port or int(os.getenv("POSTGRES_PORT", "5432"))
        self.database = database or os.getenv("POSTGRES_DB", "feast")
        self.db_schema = db_schema or os.getenv("POSTGRES_SCHEMA", "public")
        self.user = user or os.getenv("POSTGRES_USER", "feast")
        self.password = password or os.getenv("POSTGRES_PASSWORD", "feast")

        # Create SQLAlchemy engine
        self.engine = self._create_engine()
        self.session = sessionmaker(bind=self.engine)

    def _create_engine(self):
        """Create SQLAlchemy engine with proper configuration."""
        connection_string = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        return create_engine(
            connection_string,
            connect_args={"options": f"-csearch_path={self.db_schema}"},
        )

    def write_batch(
        self,
        df: pd.DataFrame,
        table_name: str,
        if_exists: str = "append",
        index: bool = False,
        chunk_size: int = 10000,
    ) -> bool:
        """
        Write a batch of data to PostgreSQL table.
        """
        try:
            if not table_name or not isinstance(table_name, str):
                raise ValueError("Invalid table name")

            with self.engine.begin() as conn:
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {self.db_schema}"))
                df.to_sql(
                    name=table_name,
                    schema=self.db_schema,
                    con=conn,
                    if_exists=if_exists,
                    index=index,
                    chunksize=chunk_size,
                    method="multi",
                )

            logger.info(
                f"Successfully wrote {len(df)} rows to {self.db_schema}.{table_name}"
            )
            return True

        except Exception as e:
            logger.error(f"Error writing batch to PostgreSQL: {str(e)}")
            return False

# Connect Kafka to Spark, then it allows to read messages from specific Kafka topics 
# kafkaDF = spark.readStream.format("kafka") \
#     .option("kafka.bootstrap.servers", "broker:29092") \
#     .option("subscribe", "training_data_streaming") \
#     .option('startingOffsets', 'earliest') \
#     .load()

# Define schema for Minio notification events received from Kafka  
# minio_event_schema = StructType([
#     StructField("EventName", StringType(), True),
#     StructField("Key", StringType(), True),
#     StructField("Records", ArrayType(
#         StructType([
#             StructField("eventName", StringType(), True),
#             StructField("s3", StructType([
#                 StructField("bucket", StructType([
#                     StructField("name", StringType(), True)
#                 ]), True),
#                 StructField("object", StructType([
#                     StructField("key", StringType(), True),
#                     StructField("size", StringType(), True),
#                     StructField("contentType", StringType(), True)
#                 ]), True)
#             ]), True)
#         ])
#     ), True)  
# ])

# Define schema for e-commerce data
ecommerce_schema = StructType([
    StructField("event_time", LongType(), True),    # Thời gian xảy ra sự kiện (UTC)
    StructField("event_type", StringType(), True),       # Loại sự kiện: view, cart, remove_from_cart, purchase
    StructField("product_id", LongType(), True),      # ID sản phẩm
    StructField("category_id", LongType(), True),     # ID danh mục sản phẩm
    StructField("category_code", StringType(), True),    # Tên danh mục (có thể null)
    StructField("brand", StringType(), True),            # Tên thương hiệu (có thể null)
    StructField("price", DoubleType(), True),            # Giá sản phẩm
    StructField("user_id", LongType(), True),         # ID người dùng
    StructField("user_session", StringType(), True)      # ID phiên làm việc của người dùng
])

# # Parse event received from Kafka
# parsed_events = kafkaDF.selectExpr("CAST(value AS STRING)") \
#         .select(from_json(col('value'), minio_event_schema).alias('event')).select("event.*")

   
def write_to_postgres(processed_df, table_name):
    """
    Ghi dữ liệu vào PostgreSQL.

    Args:
        processed_df (DataFrame): Spark DataFrame đã được xử lý.
        table_name (str): Tên bảng trong PostgreSQL.
    """
    # writer = PostgreSQLOfflineWriter(
    #     # host="postgres",
    #     host="172.18.0.2",  # Địa chỉ IP của container PostgreSQL
    #     port=5432,
    #     database="airflow",
    #     user="airflow",
    #     password="airflow"
    # )

    postgres_url = "jdbc:postgresql://172.18.0.2:5432/airflow"
    postgres_properties = {
        "user": "airflow",
        "password": "airflow",
        "driver": "org.postgresql.Driver"
    }

    # Ghi dữ liệu trực tiếp từ Spark DataFrame vào PostgreSQL
    processed_df.write.jdbc(
        url=postgres_url,
        table="processed_data",
        mode="append",
        properties=postgres_properties
    )
    # # Chuyển Spark DataFrame sang Pandas DataFrame
    # pandas_df = processed_df.toPandas()

    # # Ghi dữ liệu vào PostgreSQL
    # writer.write_batch(pandas_df, table_name=table_name)
    
    # Xử lý dữ liệu từ Spark DataFrame.

    # Args:
    #     spark_df (DataFrame): Spark DataFrame chứa dữ liệu gốc.

    # Returns:
    #     DataFrame: Spark DataFrame đã được xử lý.

def process_data(spark_df):
    # Đổi tên cột event_time thành event_timestamp nếu tồn tại
    if "event_time" in spark_df.columns:
        spark_df = spark_df.withColumnRenamed("event_time", "event_timestamp")

    # Chuyển đổi event_timestamp sang kiểu timestamp và xử lý giá trị không hợp lệ
    spark_df = spark_df.withColumn(
        "event_timestamp",
        F.to_timestamp(F.col("event_timestamp"), "yyyy-MM-dd HH:mm:ss")
    ).filter(F.col("event_timestamp").isNotNull())

    # Tính số lượng hoạt động trong mỗi phiên người dùng (activity_count)
    window_spec = Window.partitionBy("user_session")
    spark_df = spark_df.withColumn("activity_count", F.count("*").over(window_spec))

    # Tách category_code thành hai lớp: category_code_level1 và category_code_level2
    spark_df = spark_df.withColumn(
        "category_code_level1",
        F.split(F.col("category_code"), "\\.").getItem(0)
    ).withColumn(
        "category_code_level2",
        F.split(F.col("category_code"), "\\.").getItem(1)
    )

    # Tính ngày trong tuần từ event_timestamp
    spark_df = spark_df.withColumn(
        "event_weekday", F.dayofweek(F.col("event_timestamp")) - 1
    )

    # Tạo cột is_purchased
    spark_df = spark_df.withColumn(
        "is_purchased",
        F.when(F.col("event_type") == "purchase", 1).otherwise(0)
    )

    # Điền giá trị NULL bằng giá trị mặc định
    spark_df = spark_df.fillna(
        {"price": 0.0, "brand": "", "category_code_level1": "", "category_code_level2": ""}
    )

    # Lọc các bản ghi không hợp lệ
    spark_df = spark_df.filter(
        F.col("user_id").isNotNull() &
        F.col("product_id").isNotNull() &
        F.col("price").isNotNull() &
        (F.col("price") >= 0) &
        F.col("user_id").isNotNull() &
        F.col("user_session").isNotNull()
    )

    # Chọn các cột cần thiết
    feature_cols = [
        "event_timestamp",
        "user_id",
        "product_id",
        "user_session",
        "price",
        "brand",
        "category_code_level1",
        "category_code_level2",
        "event_weekday",
        "activity_count",
        "is_purchased",
    ]
    return spark_df.select(*feature_cols)
# def process_batch(batch_df, batch_id):
#     if batch_df.isEmpty():
#         logging.info(f"Batch {batch_id} is empty")
#         return
#     try:
#         # Extract bucket name & object key from event notification messages received from Kafka
#         keys_df = batch_df.filter(col("Records").isNotNull()) \
#             .select(
#                 col("Records")[0]["s3"]["bucket"]["name"].alias("bucket_name"),
#                 col("Records")[0]["s3"]["object"]["key"].alias("object_key")
#             )
        
#         if keys_df.isEmpty():
#             logging.info(f"No valid MinIO events in batch {batch_id}")
#             return   
        
#         # Xử lý từng hàng (mỗi thông báo sự kiện)
#         for row in keys_df.collect():
#             bucket_name = row["bucket_name"]
#             encoded_object_key = row["object_key"]
            
#             # URL decode key
#             object_key = urllib.parse.unquote(encoded_object_key)
            
#             logging.info(f"Processing file: {object_key} from bucket: {bucket_name}")
            
#             try:
#                 # Đọc file Parquet từ MinIO
#                 response = minio_client.get_object(bucket_name, object_key)
#                 parquet_data = response.read()
                
#                 # Ghi tạm ra file để Spark đọc (Spark cần đường dẫn file)
#                 temp_path = f"/tmp/parquet_file_{batch_id}.parquet"
#                 with open(temp_path, "wb") as f:
#                     f.write(parquet_data)
                
#                 # Đọc Parquet bằng Spark
#                 # user_df = spark.read.parquet(temp_path)
#                 user_df = spark.read.parquet("C:/Máy tính/BigDataProject/spark_app/data_2019-11-28_00.parquet")
#                 # Xử lý dữ liệu
#                 processed_df = process_data(user_df)
#                 # Ghi dữ liệu vào PostgreSQL
#                 # write_to_postgres(user_df, f"{batch_id}_{object_key}")
#                 write_to_postgres(processed_df, table_name="processed_data")

#             except Exception as e:
#                 logging(f"Error processing {object_key}: {e}")
#     except Exception as e:
#         logging.error(f"Error in batch {batch_id}: {str(e)}")
                
# query = parsed_events.writeStream \
#                     .foreachBatch(process_batch) \
#                         .outputMode("append") \
#                             .start()\
#                                 .awaitTermination()

# Simulate reading data from a local Parquet file
local_file_path = "/opt/spark_app/data_2019-11-28_00.parquet"
local_df = spark.read.schema(ecommerce_schema).parquet(local_file_path)

# Lọc các giá trị event_time không hợp lệ
local_df = local_df.filter(
    (F.col("event_time") >= 0) & (F.col("event_time") <= 253402300799)
)
# Chuyển đổi event_time từ LongType sang TimestampType
local_df = local_df.withColumn("event_time", F.from_unixtime(F.col("event_time") / 1000).cast("timestamp"))
# Process the data
processed_df = process_data(local_df)

# Write processed data to PostgreSQL
write_to_postgres(processed_df, table_name="processed_data")