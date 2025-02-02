import os

import pandas as pd
from helpers.connections import Mysql, Postgresql
from pandas import DataFrame

import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

dag_name = "ETL pipeline from MySQL to PostgreSQL"
dag_id = "mysql_to_postgresql"

default_args = {
    "owner": "data",
    "start_date": airflow.utils.dates.days_ago(7),
    "depends_on_past": False,
    "retries": 1,
    "provide_context": True,
}

CUR_DIR = os.path.abspath(os.path.dirname(__file__))

# Load environment variables
POSTGRES_DETAILS = {
    "host": "host.docker.internal",
    "port": os.environ.get("POSTGRES_DWH_PORT"),
    "db_name": os.environ.get("POSTGRES_DWH_DB"),
    "user_name": os.environ.get("POSTGRES_DWH_USER"),
    "password": os.environ.get("POSTGRES_DWH_PASSWORD"),
}
MYSQL_DETAILS = {
    "host": "host.docker.internal",
    "port": os.environ.get("MYSQL_PORT"),
    "db_name": os.environ.get("MYSQL_DB"),
    "user_name": os.environ.get("MYSQL_USER"),
    "password": os.environ.get("MYSQL_PASSWORD"),
}


def download_reference_table() -> DataFrame:
    """
    Description: Downloads reference table from PostgreSQL database.
    """
    conn_obj = Postgresql(
        host=POSTGRES_DETAILS["host"],
        port=POSTGRES_DETAILS["port"],
        db_name=POSTGRES_DETAILS["db_name"],
        user_name=POSTGRES_DETAILS["user_name"],
        password=POSTGRES_DETAILS["password"],
    )
    query = """SELECT * FROM etl_manager.database_flow_reference_table"""
    ref_table = conn_obj.execute_query(query=query, return_data=True)
    conn_obj.close_connection()  # close connection to greenplum db
    return ref_table


def extract(source_connection_name: str, schema_name: str, table_name: str, key_fields: str) -> None:
    """
    Description: Extracts given table from the development database. Saves it as csv file.

    Arguments:
        :param source_connection_name: specifies which database is going to be used for extraction.
        :param schema_name: source schema name
        :param table_name: source table name
        :param key_fields: column names to be extract from the database.
    :return:
        Outputs csv file, no return from the function.
    """
    # Connection to source database based on given parameter in the reference table
    # if you want multi-directional etl pipeline inside a single DAG, then you only have to create
    # class for that particular database and add if condition to this function.
    if source_connection_name == "mysql":
        conn_obj = Mysql(
            host=MYSQL_DETAILS["host"],
            port=MYSQL_DETAILS["port"],
            db_name=MYSQL_DETAILS["db_name"],
            user_name=MYSQL_DETAILS["user_name"],
            password=MYSQL_DETAILS["password"],
        )

    query = f"SELECT {key_fields} FROM {schema_name}.{table_name}"
    data = conn_obj.execute_query(query, return_data=True)

    file_name = f"{schema_name}_{table_name}.csv"
    file_path = CUR_DIR + "/" + file_name
    data.to_csv(file_path, index=False)

    conn_obj.close_connection()


def load_to_target(
    output_path: str, target_connection_name: str, target_schema: str, target_table: str, target_fields: str
) -> None:
    """
    Description: For csv extraction method, this function is used to read the data which is extracted from the source
    database and executes ingestion process to the target database

    Arguments:
        :param output_path: csv output path from the extraction process
        :param target_connection_name: target database name
        :param target_schema: target schema name
        :param target_table_name: target table name
        :param target_fields: columns of the target table
    Returns:
        None
    """
    if target_connection_name == "postgresql":
        conn_obj = Postgresql(
            host=POSTGRES_DETAILS["host"],
            port=POSTGRES_DETAILS["port"],
            db_name=POSTGRES_DETAILS["db_name"],
            user_name=POSTGRES_DETAILS["user_name"],
            password=POSTGRES_DETAILS["password"],
        )

    # reading data from extract_node
    data = pd.read_csv(output_path)

    # TRUNCATE TABLE
    print(f"TRUNCATING {target_schema}.{target_table}")
    conn_obj.truncate_table(table_schema=target_schema, table_name=target_table)

    print(f"Insertion started for {target_schema}.{target_table}!")
    # data insertion for each value
    conn_obj.insert_values(data=data, table_schema=target_schema, table_name=target_table, columns=target_fields)

    print(f"Inserting to {target_schema}.{target_table} is successfully completed!")
    conn_obj.close_connection()


with DAG(dag_id=dag_id, default_args=default_args, schedule_interval=None, max_active_runs=1, concurrency=1) as dag:
    dummy_start = DummyOperator(task_id="start", dag=dag)
    ref_table = download_reference_table()

    # schemas are filtered with destination names, but since all tables and schemas
    # are identical, this will not cause any problems.
    schemas = ref_table.source_schema.unique().tolist()

    ### SOURCE SCHEMA FILTERING
    for source_schema in schemas:
        schema_node = DummyOperator(task_id="source_schema" + "_" + source_schema, dag=dag)
        schema_tables = ref_table[ref_table.source_schema == source_schema].copy()
        dummy_start >> schema_node

        ## ITERATE OVER TABLE NAMES INSIDE SCHEMAS
        for idx, row in schema_tables.iterrows():
            source_connection = row["source_connection"]
            source_table = row["source_table"]
            key_fields = row["key_fields"]

            extract_node = PythonOperator(
                task_id="source_table" + "_" + source_schema + "_" + source_table,
                python_callable=extract,
                op_kwargs={
                    "source_connection_name": source_connection,
                    "schema_name": source_schema,
                    "table_name": source_table,
                    "key_fields": key_fields,
                },
                dag=dag,
            )

            destination_connection = row["destination_connection"]
            destination_schema = row["destination_schema"]
            destination_table = row["destination_table"]
            target_fields = row["target_fields"]
            output_file_path = CUR_DIR + "/" + f"{source_schema}_{source_table}.csv"

            insert_node = PythonOperator(
                task_id="destination_" + destination_schema + "_" + destination_table,
                python_callable=load_to_target,
                op_kwargs={
                    "output_path": output_file_path,
                    "target_connection_name": destination_connection,
                    "target_schema": destination_schema,
                    "target_table": destination_table,
                    "target_fields": target_fields,
                },
                dag=dag,
            )

            schema_node >> extract_node >> insert_node
