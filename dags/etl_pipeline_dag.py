import logging
import os
from typing import List

import pandas as pd
from helpers.model import download_reference_table, extract, load_to_target

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

dag_name = "ETL pipeline from MySQL to PostgreSQL"
dag_id = "mysql_to_postgresql"

default_args = {
    "owner": "data",
    "start_date": days_ago(7),
    "depends_on_past": False,
    "retries": 1,
}

CUR_DIR = os.path.abspath(os.path.dirname(__file__))


def get_schemas() -> List[str]:
    ref_table = download_reference_table()
    return ref_table.source_schema.unique().tolist()


def get_schema_tables(source_schema: str) -> pd.DataFrame:
    ref_table = download_reference_table()
    return ref_table[ref_table.source_schema == source_schema].copy()


with DAG(dag_id=dag_id, default_args=default_args, schedule_interval=None, max_active_runs=1, concurrency=1) as dag:
    start = DummyOperator(task_id="start")

    schemas = get_schemas()

    for source_schema in schemas:
        with TaskGroup(group_id=f"group_{source_schema}") as schema_group:
            schema_start = DummyOperator(task_id=f"start_{source_schema}")

            schema_tables = get_schema_tables(source_schema)

            for idx, row in schema_tables.iterrows():
                source_connection = row["source_connection"]
                source_table = row["source_table"]
                key_fields = row["key_fields"]

                extract_task = PythonOperator(
                    task_id=f"extract_{source_schema}_{source_table}",
                    python_callable=extract,
                    op_kwargs={
                        "source_connection_name": source_connection,
                        "schema_name": source_schema,
                        "table_name": source_table,
                        "key_fields": key_fields,
                    },
                )

                destination_connection = row["destination_connection"]
                destination_schema = row["destination_schema"]
                destination_table = row["destination_table"]
                target_fields = row["target_fields"]
                output_file_path = os.path.join(CUR_DIR, f"{source_schema}_{source_table}.csv")

                load_task = PythonOperator(
                    task_id=f"load_{destination_schema}_{destination_table}",
                    python_callable=load_to_target,
                    op_kwargs={
                        "output_path": output_file_path,
                        "target_connection_name": destination_connection,
                        "target_schema": destination_schema,
                        "target_table": destination_table,
                        "target_fields": target_fields,
                    },
                )

                schema_start >> extract_task >> load_task

            start >> schema_group
