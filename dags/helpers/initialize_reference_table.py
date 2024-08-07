import os
from datetime import datetime

import pandas as pd
from helpers.connections import Postgresql

CUR_DIR = os.path.abspath(os.path.dirname(__file__))


def create_database_schema_and_tables(database):
    # Initialize destination tables for data ingestion
    database.create_schema("stock")
    database.create_table(
        table_schema="stock", table_name="stock_symbols", columns={"ticker_symbol": "varchar", "stock_name": "varchar"}
    )
    database.create_table(
        table_schema="stock",
        table_name="stock_values",
        columns={
            "ticker_symbol": "varchar",
            "day_date": "timestamp",
            "close_value": "numeric",
            "volume": "bigint",
            "open_value": "float",
            "high_value": "float",
            "low_value": "float",
        },
    )
    # Initialize reference table
    database.create_schema("etl_manager")
    database.create_table(
        table_schema="etl_manager",
        table_name="database_flow_reference_table",
        columns={
            "insert_date": "timestamp",
            "source_connection": "varchar",
            "source_schema": "varchar",
            "source_table": "varchar",
            "key_fields": "varchar",
            "extraction_method": "varchar",
            "extraction_type": "varchar",
            "destination_connection": "varchar",
            "destination_schema": "varchar",
            "destination_table": "varchar",
            "target_fields": "varchar",
        },
    )
    database.truncate_table(table_schema="etl_manager", table_name="database_flow_reference_table")


def create_flow_reference_entry(
    insert_date,
    source_connection,
    source_schema,
    source_table,
    key_fields,
    extraction_method,
    extraction_type,
    destination_connection,
    destination_schema,
    destination_table,
    target_fields,
):
    return {
        "insert_date": insert_date,
        "source_connection": source_connection,
        "source_schema": source_schema,
        "source_table": source_table,
        "key_fields": key_fields,
        "extraction_method": extraction_method,
        "extraction_type": extraction_type,
        "destination_connection": destination_connection,
        "destination_schema": destination_schema,
        "destination_table": destination_table,
        "target_fields": target_fields,
    }


def insert_reference_data(database, data_dict):
    df = pd.DataFrame({k: [v] for k, v in data_dict.items()})
    database.insert_values(
        data=df,
        table_schema="etl_manager",
        table_name="database_flow_reference_table",
        columns=", ".join(df.columns.tolist()),
    )


def main():
    database = Postgresql(host="localhost", port="5433", db_name="dwh", user_name="alfian", password="alfian")

    create_database_schema_and_tables(database)

    insert_date = str(datetime.now())
    stock_symbol_dict = create_flow_reference_entry(
        insert_date=insert_date,
        source_connection="mysql",
        source_schema="stock",
        source_table="stock_symbols",
        key_fields="ticker_symbol, stock_name",
        extraction_method="jdbc",
        extraction_type="full",
        destination_connection="postgresql",
        destination_schema="stock",
        destination_table="stock_symbols",
        target_fields="ticker_symbol, stock_name",
    )

    stock_values_dict = create_flow_reference_entry(
        insert_date=insert_date,
        source_connection="mysql",
        source_schema="stock",
        source_table="stock_values",
        key_fields="ticker_symbol, day_date, close_value, volume, open_value, high_value, low_value",
        extraction_method="jdbc",
        extraction_type="full",
        destination_connection="postgresql",
        destination_schema="stock",
        destination_table="stock_values",
        target_fields="ticker_symbol, day_date, close_value, volume, open_value, high_value, low_value",
    )

    insert_reference_data(database, stock_symbol_dict)
    insert_reference_data(database, stock_values_dict)

    database.close_connection()


if __name__ == "__main__":
    main()
