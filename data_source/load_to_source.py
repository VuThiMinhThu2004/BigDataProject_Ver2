import pandas as pd 
from minio import Minio
from minio.error import S3Error
import io
import logging
import json

from minio_config import config

def upload_to_source(minio_client, bucket_name, buffer, file_name):
    
    try:
        minio_client.put_object(
            bucket_name,
            file_name, 
            data = buffer,
            length = buffer.getbuffer().nbytes,
            content_type="application/octet-stream"
        )
    except Exception as e:
        logging.error(f"Something failed when attempt to upload f{file_name}")
    

def csv_to_individual_json(minio_client, bucket_name):
    """
    Convern each row from .csv into json object and upload to data_source based on MinIO
    """
    try:
        logging.info("Starting to convert CSV rows to individual JSON files")
        
        # Read .csv file from specific path
        df = pd.read_csv('train_clean_small.csv')
        
        # Convert event_time to datetime (avoid further serialization error)
        df['event_timestamp'] = pd.to_datetime(df['event_timestamp'])
        df['event_timestamp'] = df['event_timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Adding ID collumn
        df['row_id'] = range(1, len(df) + 1)
        
        for index, row in df.iterrows():
            # Convert dataframe row to python dict
            row_dict = row.to_dict()

            # Assign new name
            file_name = f"{row_dict['row_id']}.json"
            
            if 'row_id' in row_dict:
                del row_dict['row_id']
            
            # Convert from dich to JSON format
            json_string = json.dumps(row_dict)
            
            # Buffering pre-created JSON
            buffer = io.BytesIO(json_string.encode('utf-8'))
            
            # Upload to MinIO /datasource
            upload_to_source(
                minio_client=minio_client, 
                bucket_name=bucket_name, 
                buffer=buffer, 
                file_name=file_name
            )
            
            # Tracking the progress
            if index % 100 == 0:
                logging.info(f"Processed {index} rows")
                
            if (index > 5000):
                logging.info(f"Successfully converted all {5000} rows to individual JSON files")
                break
    except Exception as e:
        logging.error(f"Error in csv_to_individual_json: {e}")

def main():
    try:
        # Config logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        logging.info("Starting data transfering")
        
        # Minio connection
        minio_client = Minio(
            "localhost:9000",
            access_key=config['access_key'],
            secret_key=config['secret_key'],
            secure=False
        )
        
        bucket_name = "datasource" 
        
        if not minio_client.bucket_exists(bucket_name=bucket_name):
            logging.warning(f"Bucket {bucket_name} does not exist")
            minio_client.make_bucket(bucket_name=bucket_name)
            logging.info(f"Successfully created bucket: {bucket_name}")
        
        logging.info("Converting CSV rows to individual JSON files...")
        csv_to_individual_json(minio_client=minio_client, bucket_name=bucket_name)
        
        logging.info("Data transfering completed successfully")
        
    except Exception as e:
        logging.error(f"Failed to transfer data: {e}")

if __name__ == "__main__":
    main()