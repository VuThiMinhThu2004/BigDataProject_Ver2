from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import json, uuid, time, logging, io, requests

from minio_config import config

#Import MinIO client
try:
    from minio import Minio
    from minio.error import S3Error
except ImportError:
    pass

default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2025, 3, 24, 10, 00),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
    
}

def get_data():
    
    res = requests.get('https://randomuser.me/api/').json()
    res = res['results'][0]
    
    return res

def format_data(res):
    data = {}
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['postcode'] = res['location']['postcode']
    data['email'] = res['email']
    data['phone'] = res['phone']
    
    return data
    
def stream_data():
    
    # Initialize MinIO client to stream data into Bronze storage
    minio_client = Minio(
        "minio:9000",
        access_key=config['access_key'],
        secret_key=config['secret_key'],
        secure=False
    )
    
    # Check the existence of bronze storage under the name "bronze"
    bucket_name = "bronze"
    if not minio_client.bucket_exists(bucket_name):
        logging.error(f"Bucket {bucket_name} does not exist")
        try:
            minio_client.make_bucket(bucket_name)
            logging.info(f"Created bucket {bucket_name}")
        except S3Error as e:
            logging.error(f"Error creating bucket {e}")
            return
    
    curr_time = time.time()
    
    while True:
        if time.time() > curr_time + 60: # More than 1 minute
            break
        try:
            res = get_data()
            
            res = format_data(res)
            
            # Create file name
            timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
            file_name = f"owo/user-{timestamp}-{uuid.uuid4().hex[:8]}.json"
            
            json_data = json.dumps(res).encode('utf-8')
            
            minio_client.put_object(
                bucket_name,
                file_name,
                io.BytesIO(json_data),
                len(json_data),
                content_type='application/json'
            )
            
            logging.info(f"Saved file {file_name} to MinIO bucket {bucket_name}")
            
            time.sleep(2)
            
        except Exception as e:
            logging.error(f'An error occured: {e}')
            continue
    
with DAG('experiment_stream', 
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    # Streaming data to Kafka
    streaming_task = PythonOperator(
        task_id='load_data_to_minio',
        python_callable=stream_data
    )
    
    # # Trigger Spark Job to process Kafka messaages then send to PostgreSQL
    # trigger_spark_job = BashOperator(
    #     task_id='trigger_spark_job',
    #     bash_command=(
    #         "docker exec -it spark-master spark-submit "
    #         "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.7.4 "
    #         "/opt/spark_app/spark_stream.py"
    #     )
    # )
    
    streaming_task







