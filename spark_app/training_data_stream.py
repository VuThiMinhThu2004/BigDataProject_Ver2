# Dựa trên schema đã định nghĩa trong Spark, bảng processed_data chứa các cột sau:

# event_timestamp: Thời gian xảy ra sự kiện.
# user_id: ID người dùng.
# product_id: ID sản phẩm.
# user_session: ID phiên làm việc của người dùng.
# price: Giá sản phẩm.
# brand: Thương hiệu sản phẩm.
# category_code_level1: Danh mục cấp 1.
# category_code_level2: Danh mục cấp 2.
# event_weekday: Ngày trong tuần (0-6).
# activity_count: Số lượng hoạt động trong phiên.
# is_purchased: Cờ đánh dấu nếu sự kiện là purchase.

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, dayofweek, when, split, count, from_unixtime

from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, ArrayType, TimestampType
from pyspark.sql import Window
from loguru import logger
import urllib.parse

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("KafkaMinIOToPostgres") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.7.4,org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.sql.shuffle.partitions", 4) \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Configure MinIO
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "minio:9000")
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "minioadmin")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "minioadmin")
spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
spark._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# Define schema for e-commerce data
ecommerce_schema = StructType([
    StructField("event_time", TimestampType(), True),  # Sửa thành TimestampType
    StructField("event_type", StringType(), True),
    StructField("product_id", LongType(), True),
    StructField("category_id", LongType(), True),
    StructField("category_code", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("user_id", LongType(), True),
    StructField("user_session", StringType(), True)
])

# Define schema for Kafka S3 event messages
kafka_event_schema = StructType([
    StructField("EventName", StringType(), True),
    StructField("Key", StringType(), True),
    StructField("Records", ArrayType(
        StructType([
            StructField("eventVersion", StringType(), True),
            StructField("eventSource", StringType(), True),
            StructField("awsRegion", StringType(), True),
            StructField("eventTime", StringType(), True),
            StructField("eventName", StringType(), True),
            StructField("s3", StructType([
                StructField("s3SchemaVersion", StringType(), True),
                StructField("configurationId", StringType(), True),
                StructField("bucket", StructType([
                    StructField("name", StringType(), True),
                    StructField("ownerIdentity", StructType([
                        StructField("principalId", StringType(), True)
                    ]), True),
                    StructField("arn", StringType(), True)
                ]), True),
                StructField("object", StructType([
                    StructField("key", StringType(), True),
                    StructField("size", LongType(), True),
                    StructField("eTag", StringType(), True),
                    StructField("contentType", StringType(), True),
                    StructField("userMetadata", StructType([
                        StructField("content-type", StringType(), True)
                    ]), True),
                    StructField("sequencer", StringType(), True)
                ]), True)
            ]), True)
        ])
    ), True)
])

def process_data(spark_df):
    try:
        logger.info(f"Processing data with {spark_df.count()} records")
        
        spark_df = spark_df.withColumnRenamed("event_time", "event_timestamp")
        spark_df = spark_df.withColumn(
            "event_timestamp",
            when(
                col("event_timestamp").contains("UTC"),
                to_timestamp(
                    regexp_replace(col("event_timestamp"), " UTC$", ""),
                    "yyyy-MM-dd HH:mm:ss"
                )
            ).otherwise(
                to_timestamp(col("event_timestamp"), "yyyy-MM-dd HH:mm:ss")
            )
        )
        logger.info(f"After timestamp conversion: {spark_df.count()} records")

        window_spec = Window.partitionBy("user_session")
        spark_df = spark_df.withColumn("activity_count", count("*").over(window_spec))
        logger.info(f"After adding activity_count: {spark_df.count()} records")

        spark_df = spark_df.withColumn(
            "category_code_level1",
            split(col("category_code"), "\\.").getItem(0)
        ).withColumn(
            "category_code_level2",
            split(col("category_code"), "\\.").getItem(1)
        )

        spark_df = spark_df.withColumn(
            "event_weekday", dayofweek(col("event_timestamp")) - 1
        )

        spark_df = spark_df.withColumn(
            "is_purchased",
            when(col("event_type") == "purchase", 1).otherwise(0)
        )

        spark_df = spark_df.fillna({
            "price": 0.0,
            "brand": "",
            "category_code_level1": "",
            "category_code_level2": ""
        })

        spark_df = spark_df.filter(
            col("user_id").isNotNull() &
            col("product_id").isNotNull() &
            col("price").isNotNull() &
            (col("price") >= 0) &
            col("user_session").isNotNull()
        )
        logger.info(f"After filtering: {spark_df.count()} records")

        feature_cols = [
            "event_timestamp", "user_id", "product_id", "user_session", "price",
            "brand", "category_code_level1", "category_code_level2", "event_weekday",
            "activity_count", "is_purchased"
        ]
        spark_df = spark_df.select(*feature_cols)
        logger.info(f"Final processed data: {spark_df.count()} records")
        return spark_df

    except Exception as e:
        logger.error(f"Error processing data: {str(e)}")
        raise

def write_to_postgres(processed_df, table_name):
    try:
        postgres_url = "jdbc:postgresql://postgres:5432/airflow"
        postgres_properties = {
            "user": "airflow",
            "password": "airflow",
            "driver": "org.postgresql.Driver"
        }
        record_count = processed_df.count()
        logger.info(f"Preparing to write {record_count} records to PostgreSQL...")
        processed_df.write.jdbc(
            url=postgres_url,
            table=table_name,
            mode="append",
            properties=postgres_properties
        )
        logger.info(f"Successfully wrote {record_count} records to {table_name}")
        processed_df.show(10)
    except Exception as e:
        logger.error(f"Error writing to PostgreSQL: {str(e)}")
        raise

def verify_postgres_data(spark, table_name):
    try:
        postgres_url = "jdbc:postgresql://postgres:5432/airflow"
        postgres_properties = {
            "user": "airflow",
            "password": "airflow",
            "driver": "org.postgresql.Driver"
        }
        df_from_postgres = spark.read.jdbc(
            url=postgres_url,
            table=table_name,
            properties=postgres_properties
        )
        record_count = df_from_postgres.count()
        logger.info(f"Found {record_count} records in PostgreSQL table {table_name}")
        df_from_postgres.show(10)
    except Exception as e:
        logger.error(f"Error reading from PostgreSQL: {str(e)}")

def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        logger.info(f"Batch {batch_id} is empty")
        return
    try:
        logger.info(f"Processing batch {batch_id}")
        # In nội dung thô của message Kafka
        batch_df.select(col("value").cast("string").alias("raw_message")).show(truncate=False)
        
        parsed_df = batch_df.select(
            from_json(col("value").cast("string"), kafka_event_schema).alias("event")
        ).select(
            col("event.Records")[0]["s3"]["bucket"]["name"].alias("bucket_name"),
            col("event.Records")[0]["s3"]["object"]["key"].alias("object_key")
        )

        parsed_df = parsed_df.filter(
            col("bucket_name").isNotNull() & col("object_key").isNotNull()
        )

        if parsed_df.isEmpty():
            logger.info(f"No valid events in batch {batch_id}")
            return

        logger.info(f"Batch {batch_id} contains {parsed_df.count()} events")
        parsed_df.show(5, truncate=False)

        for row in parsed_df.collect():
            bucket_name = row["bucket_name"]
            # Decode URL-encoded object key
            object_key = urllib.parse.unquote(row["object_key"])
            parquet_path = f"s3a://{bucket_name}/{object_key}"
            logger.info(f"Processing file: {parquet_path}")

            try:
                df = spark.read.schema(ecommerce_schema).parquet(parquet_path)
                logger.info(f"Loaded {df.count()} records from MinIO")
                df.show(10)

                processed_df = process_data(df)
                write_to_postgres(processed_df, table_name="processed_data")
                verify_postgres_data(spark, table_name="processed_data")

            except Exception as e:
                logger.error(f"Error processing {parquet_path}: {str(e)}")

    except Exception as e:
        logger.error(f"Error in batch {batch_id}: {str(e)}")

# Main execution
try:
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "broker:29092") \
        .option("subscribe", "training_data_streaming") \
        .option("startingOffsets", "earliest") \
        .load()

    query = kafka_df.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", "/tmp/spark-checkpoint") \
        .start()

    query.awaitTermination()

except Exception as e:
    logger.error(f"Error in streaming execution: {str(e)}")

finally:
    logger.info("Stopping Spark session...")
    spark.stop()