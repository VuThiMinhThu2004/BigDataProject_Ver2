import pandas as pd 
from minio import Minio
from minio.error import S3Error
import io
import logging

from minio_config import config

def upload_to_storage(minio_client, bucket_name, buffer, file_name):
    
    try:
        minio_client.put_object(
            bucket_name,
            file_name, 
            data = buffer,
            length = buffer.getbuffer().nbytes,
            content_type="application/octet-stream"
        )
        logging.info(f"Successfully uploaded {file_name} to bucket {bucket_name}")
    except Exception as e:
        logging.error(f"Something failed when attempt to upload f{file_name}")
    

def csv_to_parquet(minio_client, bucket_name):
    
    df = pd.read_csv('validated_streaming.csv')
    df['event_time'] = pd.to_datetime(df['event_time'])
    df['date_hour'] = df['event_time'].dt.strftime('%Y-%m-%d %H')

    grouped = df.groupby('date_hour')
    
    for date_hour, group in grouped:
        # Create Parquet in memory
        buffer = io.BytesIO()
        group.drop(columns=['date_hour']).to_parquet(buffer, index=False)
        
        file_name = f"data_{date_hour.replace(' ', '_')}.parquet"
        
        # Set cursor to beginner of buffer
        buffer.seek(0)
        
        # Upload to "storage" bucket
        upload_to_storage(minio_client=minio_client, bucket_name=bucket_name, buffer=buffer, file_name=file_name)

def main():
    try:
        minio_client = Minio(
            "localhost:9000",
            access_key=config['access_key'],
            secret_key=config['secret_key'],
            secure=False
        )
        
        bucket_name = "storage" 
        
        if not minio_client.bucket_exists(bucket_name=bucket_name):
            logging.warning(f"Bucket {bucket_name} does not exist")
            minio_client.make_bucket(bucket_name=bucket_name)
            logging.info(f"Successfully created bucket: {bucket_name}")
    except Exception as e:
        logging.error(f"Failed to connect to MinIO: {e}")
        
    csv_to_parquet(minio_client=minio_client, bucket_name=bucket_name)

if __name__ == "__main__":
    main()