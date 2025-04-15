import json
import io
import logging
from typing import Dict, Any, Set, Tuple

from minio import Minio
from minio.error import S3Error
from airflow.models import Variable

from botocore.config import Config
from loguru import logger
from dataclasses import dataclass
from airflow.models import Variable

from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

@task()
def check_minio_connection() -> bool:
    """Check if MinIO connection is working"""
    try:
        boto3_config = Config(max_pool_connections=128)
        s3_hook = S3Hook(aws_conn_id="minio_conn", config=boto3_config)
        s3_hook.get_conn()
        return True
    except Exception as e:
        raise Exception(f"Failed to connect to MinIO: {str(e)}")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Quản lý checkpoint: một file JSON lưu danh sách các file đã xử lý
def get_checkpoint_key() -> str:
    """Generate checkpoint key for tracking processed files."""
    return "bronze/_checkpoint.json"

# Tải checkpoint, đọc dữ liệu từ bucket storage, xử lý và lưu vào bucket bronze
def load_checkpoint(minio_client: Minio) -> Set[str]:
    """Load processed keys from checkpoint file."""
    try:
        checkpoint_key = get_checkpoint_key()
        response = minio_client.get_object("bronze", checkpoint_key)
        checkpoint_data = response.read().decode('utf-8')
        return set(json.loads(checkpoint_data).get("processed_keys", []))
    except Exception:
        logger.info("No checkpoint found, starting fresh")
        return set()

# cập nhật checkpoint 
def save_checkpoint(minio_client: Minio, processed_keys: Set[str]) -> None:
    """Save processed keys to checkpoint file."""
    try:
        checkpoint_data = json.dumps({"processed_keys": list(processed_keys)})
        checkpoint_key = get_checkpoint_key()
        minio_client.put_object(
            bucket_name="bronze",
            object_name=checkpoint_key,
            data=io.BytesIO(checkpoint_data.encode('utf-8')),
            length=len(checkpoint_data),
            content_type='application/json'
        )
        logger.info(f"Checkpoint updated with {len(processed_keys)} processed files")
    except Exception as e:
        logger.error(f"Error saving checkpoint: {e}")

# File processing utils
def process_file(minio_client: Minio, bucket_name: str, object_name: str) -> Tuple[str, bytes]:
    """Process a single file from MinIO."""
    try:
        response = minio_client.get_object(bucket_name, object_name)
        data = response.read()
        logger.info(f"Successfully processed file: {object_name}")
        return object_name, data
    except Exception as e:
        logger.error(f"Error processing file {object_name}: {e}")
        return object_name, b""


# Sửa đoạn này 
# Hàm chính sẽ được gọi trong file 'data_pipeline.py'
def ingest_raw_data(minio_client: Minio, source_bucket: str = "storage") -> Dict[str, Any]:
    """
    Ingest data from MinIO, process it, and save it to the bronze bucket.
    """
    try:
        # Load checkpoint
        processed_keys = load_checkpoint(minio_client)

        # List all files in the source bucket
        objects = list(minio_client.list_objects(bucket_name=source_bucket, recursive=True))
        all_keys = {obj.object_name for obj in objects}

        # Filter out already processed files
        keys_to_process = all_keys - processed_keys
        if not keys_to_process:
            logger.info("No new files to process")
            return {"processed_files": 0, "skipped_files": len(processed_keys)}

        logger.info(f"Found {len(keys_to_process)} new files to process")

        # Process files
        dest_bucket = "bronze"
        newly_processed_keys = set()
        for key in keys_to_process:
            object_name, data = process_file(minio_client, source_bucket, key)
            if data:
                # Save processed data to the bronze bucket
                dest_file = f"processed/{object_name}"
                minio_client.put_object(
                    bucket_name=dest_bucket,
                    object_name=dest_file,
                    data=io.BytesIO(data),
                    length=len(data),
                    content_type='application/octet-stream'
                )
                newly_processed_keys.add(object_name)

        # Update checkpoint
        processed_keys.update(newly_processed_keys)
        save_checkpoint(minio_client, processed_keys)

        logger.info(f"Successfully processed {len(newly_processed_keys)} files")
        return {"processed_files": len(newly_processed_keys), "skipped_files": len(processed_keys)}

    except Exception as e:
        logger.error(f"Error during ingestion: {e}")
        return {"processed_files": 0, "error": str(e)}
    
@dataclass
class DataPipelineConfig:
    bucket_name: str
    path_prefix: str
    schema_registry_url: str
    schema_subject: str
    batch_size: int

    def __post_init__(self):
        """Validate the configuration"""
        if not self.bucket_name:
            raise ValueError("bucket_name cannot be empty")
        if not self.path_prefix:
            raise ValueError("path_prefix cannot be empty")
        if self.batch_size <= 0:
            raise ValueError("batch_size must be positive")

    @classmethod
    def from_airflow_variables(cls):
        """Load configuration from Airflow Variables"""
        return cls(
            bucket_name=Variable.get(
                "MINIO_BUCKET_NAME", default_var="validated-events-bucket"
            ),
            path_prefix=Variable.get(
                "MINIO_PATH_PREFIX",
                default_var="topics/tracking.user_behavior.validated/year=2025/month=01",
            ),
            schema_registry_url=Variable.get(
                "SCHEMA_REGISTRY_URL", default_var="http://schema-registry:8081"
            ),
            schema_subject=Variable.get(
                "SCHEMA_SUBJECT", default_var="user_behavior_raw_schema_v1"
            ),
            batch_size=int(Variable.get("BATCH_SIZE", default_var="1000")),
        )