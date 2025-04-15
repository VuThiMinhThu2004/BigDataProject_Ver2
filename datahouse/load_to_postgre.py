from typing import Any, Dict

import pandas as pd
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from db_utils.dim_schemas import (
    DimCategorySchema,
    DimDateSchema,
    DimProductSchema,
    DimUserSchema,
)
from db_utils.fact_schemas import FactEventSchema
from utils.sql_utils import load_sql_template
from loguru import logger
from typing import Any

import pandas as pd
from psycopg2.extras import execute_values

from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logger.bind(name=__name__)

##### BASE FUNCTIONS #####
def create_analytical_views(postgres_hook: PostgresHook) -> None:
    """Create useful views for analysis"""
    logger.info("Creating analytical views")
    try:
        # Load and execute SQL file
        sql = load_sql_template("views/analytical_views.sql")
        postgres_hook.run(sql)

        # Verify views exist
        views = [
            "vw_user_session_summary",
            "vw_category_performance",
        ]
        for view_name in views:
            verification_sql = f"""
            SELECT EXISTS (
                SELECT FROM pg_views
                WHERE schemaname = 'airflow'
                AND viewname = '{view_name}'
            );
            """
            exists = postgres_hook.get_first(verification_sql)[0]
            if exists:
                logger.info(f"Successfully verified view exists: {view_name}")
            else:
                logger.error(f"View was not created: {view_name}")

    except Exception as e:
        logger.error(f"Failed to create analytical views: {str(e)}")
        raise Exception(f"Failed to create analytical views: {str(e)}")
    
def create_schema_and_table(
    postgres_hook: PostgresHook, schema_class: Any, table_name: str
) -> None:
    """Create table and indexes based on schema definition"""
    try:
        # Create table
        columns = [f"{col} {dtype}" for col, dtype in schema_class.table_schema.items()]
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join(columns)}
        );
        """
        postgres_hook.run(create_table_sql)

        # Create indexes with IF NOT EXISTS
        for index_sql in schema_class.indexes:
            # Add IF NOT EXISTS to index creation
            if "CREATE INDEX" in index_sql:
                index_sql = index_sql.replace(
                    "CREATE INDEX", "CREATE INDEX IF NOT EXISTS"
                )
            postgres_hook.run(index_sql)

    except Exception as e:
        raise AirflowException(f"Failed to create schema and table: {str(e)}")


def batch_insert_data(
    postgres_hook: PostgresHook, df: pd.DataFrame, table_name: str
) -> None:
    """Insert data in batches"""
    try:
        # Convert DataFrame to native Python types
        df = df.astype(object)  # Convert all columns to object type first
        df = df.where(pd.notnull(df), None)  # Replace NaN with None

        # Get connection and cursor
        conn = postgres_hook.get_conn()
        cur = conn.cursor()

        # Get column names
        columns = df.columns.tolist()

        # Convert DataFrame to list of tuples
        values = df.to_records(index=False).tolist()

        # Construct insert query
        insert_query = f"""
            INSERT INTO {table_name} ({','.join(columns)})
            VALUES %s
            ON CONFLICT DO NOTHING;
        """

        # Execute in batches using execute_values
        batch_size = 1000
        execute_values(cur, insert_query, values, page_size=batch_size)

        # Commit the transaction
        conn.commit()

    except Exception as e:
        raise AirflowException(f"Failed to insert data: {str(e)}")
    finally:
        if "cur" in locals():
            cur.close()

#### FUNCTIONS TO CREATE IN DATABASE #####
def create_dim_user(df: pd.DataFrame) -> pd.DataFrame:
    """Create user dimension table"""
    logger.info("Creating user dimension table")
    dim_user = df[["user_id"]].copy()
    dim_user.loc[:, "user_id"] = dim_user["user_id"].astype(int)
    return dim_user.drop_duplicates()


def create_dim_product(df: pd.DataFrame) -> pd.DataFrame:
    """Create product dimension table"""
    logger.info("Creating product dimension table")
    dim_product = df[["product_id", "brand", "price", "price_tier"]].copy()
    dim_product.loc[:, "product_id"] = dim_product["product_id"].astype(int)
    dim_product.loc[:, "price"] = dim_product["price"].astype(float)
    return dim_product.drop_duplicates()


def create_dim_category(df: pd.DataFrame) -> pd.DataFrame:
    """Create category dimension table"""
    logger.info("Creating category dimension table")
    dim_category = df[
        ["category_id", "category_code", "category_l1", "category_l2", "category_l3"]
    ].copy()
    dim_category.loc[:, "category_id"] = dim_category["category_id"].astype(int)
    return dim_category.drop_duplicates()


def create_dim_date(df: pd.DataFrame) -> pd.DataFrame:
    """Create date dimension table"""
    logger.info("Creating date dimension table")
    dim_date = df[["event_date", "event_hour", "day_of_week"]].copy()
    return dim_date.drop_duplicates()


def create_fact_events(df: pd.DataFrame, dims: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Create fact table with foreign keys to dimensions"""
    logger.info("Creating fact events table")
    fact_events = df[
        [
            "event_type",
            "user_id",
            "product_id",
            "category_id",
            "event_date",
            "event_timestamp",
            "user_session",
            "events_in_session",
        ]
    ].copy()

    fact_events.loc[:, "user_id"] = fact_events["user_id"].astype(int)
    fact_events.loc[:, "product_id"] = fact_events["product_id"].astype(int)
    fact_events.loc[:, "category_id"] = fact_events["category_id"].astype(int)
    fact_events.loc[:, "events_in_session"] = fact_events["events_in_session"].astype(
        int
    )

    return fact_events

###### MAIN FUNCTION WILL BE RAN IN DATA_PIPELINE
@task()
def load_dimensions_and_facts(transformed_data: Dict[str, Any]) -> Dict[str, Any]:
    """Load dimensional model into Data Warehouse"""
    logger.info("Loading dimensions and facts into Data Warehouse")
    try:
        postgres_hook = PostgresHook(postgres_conn_id="postgres")
        df = pd.DataFrame(transformed_data["data"])

        # Create schema if not exists
        postgres_hook.run("CREATE SCHEMA IF NOT EXISTS airflow;")

        # Create dimensions
        dims = {
            "airflow.dim_user": create_dim_user(df),
            "airflow.dim_product": create_dim_product(df),
            "airflow.dim_category": create_dim_category(df),
            "airflow.dim_date": create_dim_date(df),
        }

        # Map table names to schema classes
        schema_mapping = {
            "airflow.dim_user": DimUserSchema,
            "airflow.dim_product": DimProductSchema,
            "airflow.dim_category": DimCategorySchema,
            "airflow.dim_date": DimDateSchema,
        }

        # Create fact table
        fact_events = create_fact_events(df, dims)

        # Create and load dimension tables
        for table_name, dim_df in dims.items():
            schema_class = schema_mapping[table_name]
            create_schema_and_table(postgres_hook, schema_class, table_name)
            batch_insert_data(postgres_hook, dim_df, table_name)

        # Create and load fact table
        create_schema_and_table(postgres_hook, FactEventSchema, "airflow.fact_events")
        batch_insert_data(postgres_hook, fact_events, "airflow.fact_events")

        # Create useful views
        create_analytical_views(postgres_hook)

        return {
            "data": df,
            "success": True,
        }

    except Exception as e:
        logger.error(f"Failed to load dimensional model: {str(e)}")
        raise Exception(f"Failed to load dimensional model: {str(e)}")