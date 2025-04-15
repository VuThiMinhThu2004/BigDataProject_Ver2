from datetime import timedelta
from typing import Any, Dict

import pandas as pd
import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.utils.task_group import TaskGroup
from datahouse.ingest_data import ingest_raw_data, check_minio_connection
from datahouse.validate_data import validate_raw_data
from datahouse.transform_data import transform_data
from datahouse.load_to_postgre import load_dimensions_and_facts
from datahouse.ingest_data import DataPipelineConfig

from loguru import logger
logger = logger.bind(name=__name__)

# Constants for Great Expectations
POSTGRES_CONN_ID = "airflow"
POSTGRES_SCHEMA = "airflow"
# GX_DATA_CONTEXT = "include/gx"

# Define default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=60),
    "execution_timeout": timedelta(hours=1),
    "sla": timedelta(hours=2),
}


def bronze_layer(config: DataPipelineConfig) -> Dict[str, Any]:
    """Task group for the bronze layer of the data pipeline."""
    # Add pre-execution checks
    @task(task_id="check_prerequisites")
    def check_prerequisites():
        """Check all prerequisites before starting the layer"""
        # Check MinIO connection
        valid = check_minio_connection()
        if not valid:
            raise AirflowException("MinIO connection failed")
        return valid

    # Add retries and timeouts to critical tasks
    @task(
        task_id="ingest_raw_data",
        retries=3,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(minutes=30),
    )
    def ingest_data(config: DataPipelineConfig, valid: bool) -> Dict[str, Any]:
        return ingest_raw_data(config, valid)

    @task(
        task_id="quality_check_raw_data",
        retries=2,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=15),
    )
    def validate_data(raw_data: Dict[str, Any]) -> Dict[str, Any]:
        if raw_data is None:
            logger.warning("Raw data is None, skipping validation")
            return {"data": [], "metrics": {"total_records": 0, "valid_records": 0}}
        return validate_raw_data(raw_data)

    # Check MinIO connection
    valid = check_minio_connection()
    if not valid:
        logger.error("MinIO connection failed.")
        return {"data": [], "metrics": {"total_records": 0, "valid_records": 0}}

    # Ingest raw data
    raw_data = ingest_raw_data(config, valid)
    if raw_data is None:
        logger.error("Ingested raw data is None.")
        return {"data": [], "metrics": {"total_records": 0, "valid_records": 0}}

    # Validate raw data
    validated_data = validate_raw_data(raw_data)
    if validated_data is None:
        logger.error("Validation of raw data failed.")
        return {"data": [], "metrics": {"total_records": 0, "valid_records": 0}}

    return validated_data


def silver_layer(validated_data: Dict[str, Any]) -> Dict[str, Any]:
    """Task group for the silver layer of the data pipeline."""
    if validated_data is None or not validated_data["data"]:
        logger.warning("No valid data to transform")
        return {"data": [], "metrics": {"transformed_records": 0}}

    # Transform data
    transformed_data = transform_data(validated_data)

    if transformed_data["skip"] is False:
        logger.warning(transformed_data["message"])
        return {"data": [], "metrics": {"transformed_records": 0}}

    return transformed_data


def gold_layer(transformed_data: Dict[str, Any]) -> pd.DataFrame:
    """Task group for the gold layer of the data pipeline."""
    if transformed_data is None:
        logger.error("Transformed data is None.")
        return False

    # Load dimensions and facts
    transformed_data = load_dimensions_and_facts(transformed_data)
    if not transformed_data["success"]:
        logger.error("Failed to load dimensional model.")
        return False

    return transformed_data["data"]


@task
def debug_data(data: Dict[str, Any], layer: str):
    """Debug task to inspect data between layers"""
    if data and "data" in data:
        df = pd.DataFrame(data["data"])
        logger.info(f"=== {layer} Layer Data ===")
        logger.info(f"Columns: {df.columns.tolist()}")
        logger.info(f"Shape: {df.shape}")
        logger.info(f"First row: {df.iloc[0].to_dict()}")
    return data

@dag(
    dag_id="data_pipeline",
    default_args=default_args,
    description="Data pipeline for processing events from MinIO to DWH",
    schedule="@hourly",
    start_date=pendulum.datetime(2025, 4, 15, tz="UTC"),
    catchup=False,
    tags=["data_lake", "data_warehouse"],
    max_active_runs=3,
    doc_md=__doc__,
)
def data_pipeline():
    """
    ### Data Pipeline DAG

    This DAG processes data through three layers:
    * Bronze: Raw data ingestion and validation
    * Silver: Data transformation and enrichment
    * Gold: Loading to dimensional model with data quality validation

    Dependencies:
    * MinIO connection
    * Postgres DWH connection
    """
    # Load configuration
    config = DataPipelineConfig.from_airflow_variables()

    # Execute layers with proper error handling
    with TaskGroup("bronze_layer_group") as bronze_group:
        validated_data = bronze_layer(config)
        # validated_data = debug_data(validated_data, "Bronze")

    with TaskGroup("silver_layer_group") as silver_group:
        transformed_data = silver_layer(validated_data)

    # Gold layer tasks
    with TaskGroup("gold_layer_group") as gold_group:
        gold_data = gold_layer(transformed_data)  # noqa: F841
        # quality_check_gold_data(gold_data)

    # # Define dependencies
    # bronze_group >> silver_group >> gold_group    
    bronze_group >> silver_group >> gold_group



# Create DAG instance
data_pipeline_dag = data_pipeline()
