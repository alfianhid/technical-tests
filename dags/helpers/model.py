import logging
import os
from typing import Dict

import pandas as pd
from helpers.connections import Mysql, Postgresql
from pandas import DataFrame

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CUR_DIR = os.path.abspath(os.path.dirname(__file__))
POSTGRE_DETAILS: Dict[str, str] = {
    "host": "host.docker.internal",
    "port": "5433",
    "db_name": "dwh",
    "user": "alfian",
    "password": "alfian",
}
MYSQL_DETAILS: Dict[str, str] = {
    "host": "host.docker.internal",
    "port": "3306",
    "db_name": "staging",
    "user": "alfian",
    "password": "alfian",
}


def get_postgresql_connection() -> Postgresql:
    return Postgresql(
        host=POSTGRE_DETAILS["host"],
        port=POSTGRE_DETAILS["port"],
        db_name=POSTGRE_DETAILS["db_name"],
        user_name=POSTGRE_DETAILS["user"],
        password=POSTGRE_DETAILS["password"],
    )


def get_mysql_connection() -> Mysql:
    return Mysql(
        host=MYSQL_DETAILS["host"],
        port=MYSQL_DETAILS["port"],
        db_name=MYSQL_DETAILS["db_name"],
        user_name=MYSQL_DETAILS["user"],
        password=MYSQL_DETAILS["password"],
    )


def download_reference_table() -> DataFrame:
    logger.info("Starting to download reference table")
    query = """SELECT * FROM etl_manager.database_flow_reference_table"""
    try:
        with get_postgresql_connection() as conn:
            logger.info(f"Executing query: {query}")
            ref_table = conn.execute_query(query=query, return_data=True)
            logger.info("Query executed successfully")
            return ref_table
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        raise


def extract(source_connection_name: str, schema_name: str, table_name: str, key_fields: str) -> None:
    connection = get_mysql_connection() if source_connection_name == "mysql" else None
    if not connection:
        logger.error(f"Unsupported connection name: {source_connection_name}")
        return

    query = f"SELECT {key_fields} FROM {schema_name}.{table_name}"
    try:
        with connection as conn:
            logger.info(f"Executing query: {query}")
            data = conn.execute_query(query, return_data=True)
            file_path = os.path.join(CUR_DIR, f"{schema_name}_{table_name}.csv")
            data.to_csv(file_path, index=False)
            logger.info(f"Data extracted and saved to {file_path}")
    except Exception as e:
        logger.error(f"Error during extraction: {e}")
        raise


def load_to_target(
    output_path: str, target_connection_name: str, target_schema: str, target_table: str, target_fields: str
) -> None:
    connection = get_postgresql_connection() if target_connection_name == "postgresql" else None
    if not connection:
        logger.error(f"Unsupported connection name: {target_connection_name}")
        return

    try:
        data = pd.read_csv(output_path)
        with connection as conn:
            logger.info(f"TRUNCATING {target_schema}.{target_table}")
            conn.truncate_table(table_schema=target_schema, table_name=target_table)
            logger.info(f"Inserting data into {target_schema}.{target_table}")
            conn.insert_values(data=data, table_schema=target_schema, table_name=target_table, columns=target_fields)
            logger.info(f"Data inserted into {target_schema}.{target_table} successfully")
    except Exception as e:
        logger.error(f"Error during loading data: {e}")
        raise
