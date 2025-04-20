from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import logging, io

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

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

def minio_client_initialization():
    
    minio_client = Minio(
        'minio:9000',
        access_key=config['access_key'],
        secret_key=config['secret_key'],
        secure=False
    )
    
    bucket_name = "bronze"
    
    if not minio_client.bucket_exists(bucket_name=bucket_name):
        try:
            minio_client.make_bucket(bucket_name=bucket_name)
            logging.info(f"Create bucket {bucket_name} successfully")
        except S3Error as e:
            logging.error(f"Failed to create bucket {bucket_name}")
    
    return minio_client

def get_data(minio_client: Minio, bucket_name='datasource'):
    
    if not minio_client.bucket_exists(bucket_name):
        logging.error(f"Bucket {bucket_name} does not exist")
        return

    try:
        last_accessed_element = Variable.get("last_accessed_element")
    except:
        last_accessed_element = 0
        Variable.set("last_accessed_element", last_accessed_element)
    
    logging.info(f"Last accessed element: {last_accessed_element}")
    
    curr_accessed_element = int(last_accessed_element) + 1
    
    target_filename = f"{curr_accessed_element}.json"
    
    try:
        objects = list(minio_client.list_objects(bucket_name=bucket_name, prefix=target_filename))
        if not objects:
            logging.warning(f"File {target_filename} does not exist in bucket {bucket_name}")
            return f"File {target_filename} does not exist, will try again next run"
    except Exception as e:
        logging.error(f"Error listing objects: {e}")
        return
        
    try:
        response = minio_client.get_object(bucket_name=bucket_name, object_name=target_filename)
        dest_file = f"data/{curr_accessed_element}.json"
        return response, dest_file, curr_accessed_element
    except Exception as e:
        logging.error(f"Something went wrong when read object: {e}")
        return

    
def stream_data():
    
    minio_client = minio_client_initialization()
    
    response, dest_file, curr_accessed_hour = get_data(minio_client=minio_client)
    
    dest_bucket = "bronze"
    
    try:
        buffer = response.read()
        
        minio_client.put_object(
        bucket_name=dest_bucket,
        object_name=dest_file,
        data=io.BytesIO(buffer),
        length=len(buffer),
        content_type='application/octet-stream'
        )
        
        Variable.set('last_accessed_element', curr_accessed_hour)
    except Exception as e:
        logging.error(f"Something went wrong when trying to put data to bronze {e}")
        
with DAG('data_streaming', 
         default_args=default_args,
         schedule_interval=timedelta(minutes=1),
         catchup=False) as dag:

    # Streaming data to bronze storage
    streaming_task = PythonOperator(
        task_id='load_data_bronze',
        python_callable=stream_data
    )
    
    streaming_task