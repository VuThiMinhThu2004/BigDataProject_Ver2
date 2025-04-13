import pandas as pd 
from minio import Minio
from minio.error import S3Error
import io
import logging
import json

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
        # logging.info(f"Successfully uploaded {file_name} to bucket {bucket_name}")
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
        
        file_name = f"traing_data/data_{date_hour.replace(' ', '_')}.parquet"
        
        # Set cursor to beginner of buffer
        buffer.seek(0)
        
        # Upload to "storage" bucket
        upload_to_storage(minio_client=minio_client, bucket_name=bucket_name, buffer=buffer, file_name=file_name)

def csv_to_individual_json(minio_client, bucket_name):
    """
    Chuyển từng hàng của DataFrame thành file JSON riêng biệt và lưu vào MinIO
    """
    try:
        logging.info("Starting to convert CSV rows to individual JSON files")
        
        # Đọc file CSV
        df = pd.read_csv('validated_streaming.csv')
        
        # Chuyển event_time thành datetime sau đó thành chuỗi để tránh lỗi serialization
        df['event_time'] = pd.to_datetime(df['event_time'])
        df['event_time'] = df['event_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        df = df.dropna(axis=0)
        
        # Tạo thêm cột ID duy nhất cho mỗi hàng nếu chưa có
        df['row_id'] = range(1, len(df) + 1)
        
        # Xử lý từng hàng
        for index, row in df.iterrows():
            # Chuyển đổi hàng thành dict
            row_dict = row.to_dict()

            # Tạo cấu trúc thư mục phân cấp và tên file
            file_name = f"testing_data/{row_dict['row_id']}.json"
            
            # Loại bỏ row_id từ dữ liệu cuối cùng nếu không muốn lưu
            if 'row_id' in row_dict:
                del row_dict['row_id']
            
            # Chuyển dict thành chuỗi JSON
            json_string = json.dumps(row_dict)
            
            # Tạo buffer từ chuỗi JSON
            buffer = io.BytesIO(json_string.encode('utf-8'))
            
            # Upload lên MinIO
            upload_to_storage(
                minio_client=minio_client, 
                bucket_name=bucket_name, 
                buffer=buffer, 
                file_name=file_name
            )
            
            if index % 100 == 0:  # Log cứ mỗi 100 hàng
                logging.info(f"Processed {index} rows")
                
            if (index > 5000):
                logging.info(f"Successfully converted all {5000} rows to individual JSON files")
                break
    except Exception as e:
        logging.error(f"Error in csv_to_individual_json: {e}")

def main():
    try:
        # Thiết lập logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        logging.info("Starting data processing")
        
        # Kết nối MinIO
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
        
        # Chuyển từng hàng của CSV thành file JSON riêng biệt
        logging.info("Converting CSV rows to individual JSON files...")
        csv_to_individual_json(minio_client=minio_client, bucket_name=bucket_name)
        
        # Chuyển CSV thành Parquet
        logging.info("Converting CSV to Parquet...")
        csv_to_parquet(minio_client=minio_client, bucket_name=bucket_name)
        
        logging.info("Data processing completed successfully")
        
    except Exception as e:
        logging.error(f"Failed to process data: {e}")

if __name__ == "__main__":
    main()