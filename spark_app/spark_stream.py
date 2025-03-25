from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder \
    .appName("KafkaExample") \
    .config("spark.streaming.stopGracefullyonShutdown", True)\
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0")\
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4") \
    .config("spark.sql.shuffle.partitions", 4)\
    .master("spark://spark-master:7077")\
    .getOrCreate()
    
kafkaDF = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", "users.created") \
    .option('startingOffsets', 'earliest') \
    .load()
    
schema = StructType([
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("postcode", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True)
])

parsed_df = kafkaDF.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
        
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
            .option("createTableColumnTypes", "first_name VARCHAR(255), last_name VARCHAR(255), gender VARCHAR(50), postcode VARCHAR(50), email VARCHAR(255), phone VARCHAR(50)") \
            .save()
        print(f"Batch {batch_id} loaded successfully")
    except Exception as e:
        print(f"Error writing batch {batch_id}: {str(e)}")

# Define a query to postgre table: employees
query = parsed_df.writeStream \
                    .foreachBatch(write_to_postgres) \
                        .outputMode("append") \
                            .start()\
                                .awaitTermination()