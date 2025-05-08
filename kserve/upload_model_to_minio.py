import boto3
import os
from load_latest import find_latest_model
import xgboost as xgb

# def convert_ubj_to_bst(ubj_path, bst_path):
#     model = xgb.Booster()
#     model.load_model(ubj_path)
#     model.save_model(bst_path)
#     print(f"Converted {ubj_path} to {bst_path}")
    
def upload_model_to_minio(path_to_model, minio_endpoint, access_key, secret_key):

    #MinIO client
    s3 = boto3.resource(
        's3',
        endpoint_url=minio_endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )
    
    
    bucket_name = 'models'
    s3_key = os.path.basename(path_to_model)
    
    bucket = s3.Bucket(bucket_name)
    if not bucket.creation_date:
        print(f"Bucket '{bucket_name}' doesn't exist. Creating it now...")
        bucket = s3.create_bucket(Bucket=bucket_name)
    
    print(f"Uploading model from {path_to_model} to {bucket_name}/{s3_key}")
    bucket.upload_file(path_to_model, s3_key)
    print("Upload completed.")

ubj_path = find_latest_model()
# bst_path = "kserve/bst/" + os.path.basename(ubj_path).replace('.ubj', '.bst')
# convert_ubj_to_bst(ubj_path, bst_path)

# test_path = "xgboost_iris.ubj"

upload_model_to_minio(
    path_to_model=ubj_path,  
    minio_endpoint='http://localhost:9000',  
    access_key='minioadmin', 
    secret_key='minioadmin'  
)