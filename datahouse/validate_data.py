import json
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
from hashlib import sha256

import pandas as pd
import pendulum
from minio import Minio
from airflow.decorators import task

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def extract_payload(record: Dict[str, Any]) -> Dict[str, Any]:
    """Extract payload from nested record structure"""
    try:
        if isinstance(record.get("payload"), dict):
            return record["payload"]
        return record
    except Exception:
        return record


def validate_record(record: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """Validate a single record against business rules"""
    try:
        # Extract payload if nested
        data = extract_payload(record)

        # Check if event_time exists (timestamp field)
        if not data.get("event_time"):
            logger.error("Missing event_time for record: %s", data)
            return False, "Missing event_time"

        # Check required fields
        if not data.get("event_type"):
            logger.error("Missing event_type for record: %s", data)
            return False, "Missing event_type"

        if not data.get("product_id"):
            logger.error("Missing product_id for record: %s", data)
            return False, "Missing product_id"

        if not data.get("user_id"):
            logger.error("Missing user_id for record: %s", data)
            return False, "Missing user_id"
            
        # Validate event_type
        event_type = data.get("event_type")
        valid_event_types = ["view", "cart", "purchase", "remove_from_cart"]
        if event_type and event_type not in valid_event_types:
            logger.error(f"Invalid event_type: {event_type} for record: {data}")
            return False, f"Invalid event_type: {event_type}, must be one of {valid_event_types}"
        
        # Validate price is positive if present
        price = data.get("price")
        if price is not None and not (isinstance(price, (int, float)) and price >= 0):
            logger.error(f"Invalid price: {price} for record: {data}")
            return False, f"Price must be a positive number"
            
        # Validate user_id and product_id are positive integers
        user_id = data.get("user_id")
        if user_id and not (isinstance(user_id, (int)) and user_id > 0):
            logger.error(f"Invalid user_id: {user_id} for record: {data}")
            return False, f"user_id must be a positive integer"
            
        product_id = data.get("product_id")
        if product_id and not (isinstance(product_id, (int)) and product_id > 0):
            logger.error(f"Invalid product_id: {product_id} for record: {data}")
            return False, f"product_id must be a positive integer"
            
        # Validate string fields aren't empty if present
        for string_field in ["brand", "category_code", "user_session"]:
            value = data.get(string_field)
            if value is not None and isinstance(value, str) and value.strip() == "":
                logger.error(f"Empty {string_field} for record: {data}")
                return False, f"{string_field} cannot be empty if present"

        return True, None
    except Exception as e:
        logger.exception("Error validating record: %s", record)
        return False, str(e)


def generate_record_hash(record: Dict[str, Any]) -> str:
    """Generate a unique hash for a record based on business keys"""
    try:
        data = extract_payload(record)
        key_fields = ["product_id", "event_time", "user_id"]
        key_string = "|".join(str(data.get(field, "")) for field in key_fields)
        return sha256(key_string.encode()).hexdigest()
    except Exception as e:
        logger.error(f"Error generating hash for record: {str(e)}")
        return sha256(str(record).encode()).hexdigest()


def enrich_record(record: Dict[str, Any], record_hash: str) -> Dict[str, Any]:
    """Add metadata to a record"""
    try:
        # Keep original record structure
        enriched = record.copy()
        enriched["processed_date"] = datetime.now(tz=pendulum.timezone("UTC")).isoformat()
        enriched["processing_pipeline"] = "minio_etl"
        enriched["valid"] = "TRUE"
        enriched["record_hash"] = record_hash

        # If payload exists, also add metadata there
        if isinstance(enriched.get("payload"), dict):
            enriched["payload"]["processed_date"] = enriched["processed_date"]
            enriched["payload"]["processing_pipeline"] = enriched["processing_pipeline"]
            enriched["payload"]["valid"] = enriched["valid"]
            enriched["payload"]["record_hash"] = enriched["record_hash"]

        return enriched
    except Exception as e:
        logger.error(f"Error enriching record: {str(e)}")
        return record


def flatten_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten nested record structure"""
    try:
        flattened = {}
        # Copy top-level metadata
        for key in ["record_hash", "processed_date", "processing_pipeline", "valid"]:
            if key in record:
                flattened[key] = record[key]

        # Extract payload data
        payload = record.get("payload", {})
        if isinstance(payload, dict):
            for key, value in payload.items():
                flattened[key] = value

        return flattened
    except Exception as e:
        logger.error(f"Error flattening record: {str(e)}")
        return record

@task(task_id="quality_check_raw_data")
def validate_raw_data(minio_client: Minio, bronze_bucket: str = "bronze") -> Dict[str, Any]:
    """Validate raw data using custom Python validation"""
    try:
        # Find all processed files in the bronze bucket from previous step
        objects = list(minio_client.list_objects(bucket_name=bronze_bucket, prefix="processed/"))
        if not objects:
            logger.info("No files to validate")
            return {"validated_files": 0}
        
        logger.info(f"Found {len(objects)} files to validate")
        
        # Process each file
        all_data = []
        validated_files = 0
        
        for obj in objects:
            try:
                # Get object data
                response = minio_client.get_object(bronze_bucket, obj.object_name)
                file_content = response.read()
                
                # Parse file content as JSON or Parquet
                try:
                    data = json.loads(file_content)
                    if isinstance(data, dict):
                        all_data.append(data)
                    elif isinstance(data, list):
                        all_data.extend(data)
                except json.JSONDecodeError:
                    # If not JSON, try to parse as parquet
                    try:
                        import io
                        import pyarrow.parquet as pq
                        parquet_file = io.BytesIO(file_content)
                        table = pq.read_table(parquet_file)
                        df = table.to_pandas()
                        all_data.extend(df.to_dict('records'))
                    except Exception as e:
                        logger.error(f"Error parsing file {obj.object_name}: {e}")
                        continue
                
                validated_files += 1
                logger.info(f"Successfully loaded {obj.object_name}")
                
            except Exception as e:
                logger.error(f"Error processing {obj.object_name}: {e}")
                continue
                
        if not all_data:
            logger.warning("No valid data found in files")
            return {"validated_files": 0, "error": "No valid data found"}
            
        # Convert data to DataFrame for validation
        df = pd.DataFrame(all_data)
        logger.info(f"Initial columns: {df.columns.tolist()}")
        
        # Add record hash
        df["record_hash"] = df.apply(lambda x: generate_record_hash(x.to_dict()), axis=1)
        
        # Validate records
        records = df.to_dict("records")
        validation_results = [validate_record(record) for record in records]
        valid_records = [
            record
            for record, (is_valid, _) in zip(records, validation_results)
            if is_valid
        ]
        validation_errors = {
            i: error
            for i, (is_valid, error) in enumerate(validation_results)
            if not is_valid
        }
        
        # Calculate metrics
        metrics = {
            "total_records": len(df),
            "valid_records": len(valid_records),
            "invalid_records": len(df) - len(valid_records),
            "validation_errors_count": len(validation_errors),
            "validated_files": validated_files
        }
        
        logger.info(f"Validation metrics: {metrics}")
            
        # Enrich and flatten valid records
        enriched_data = []
        for record in valid_records:
            enriched = enrich_record(record, record["record_hash"])
            flattened = flatten_record(enriched)
            enriched_data.append(flattened)
            
        # Save validated data to a new location in the bronze bucket
        for i, record in enumerate(enriched_data):
            file_name = f"validated/record_{i}_{record['record_hash']}.json"
            record_json = json.dumps(record)
            minio_client.put_object(
                bucket_name=bronze_bucket,
                object_name=file_name,
                data=io.BytesIO(record_json.encode('utf-8')),
                length=len(record_json),
                content_type='application/json'
            )
            
        return {"data": enriched_data, "metrics": metrics}
            
    except Exception as e:
        logger.exception("Failed to validate data")
        return {"validated_files": 0, "error": str(e)}