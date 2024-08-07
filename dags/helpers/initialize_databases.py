import os

import pandas as pd
from helpers.connections import Mysql

# Constants
CUR_DIR = os.path.abspath(os.path.dirname(__file__))
DB_CONFIG = {"host": "localhost", "port": 3306, "db_name": "staging", "user_name": "alfian", "password": "alfian"}


# Functions
def initialize_database(database):
    """Initialize the database schema and tables."""
    database.drop_table(table_schema="stock", table_name="stock_symbols")
    database.drop_table(table_schema="stock", table_name="stock_values")
    database.drop_schema(table_schema="stock")
    database.create_schema("stock")


def load_data(file_path):
    """Load data from a CSV file."""
    return pd.read_csv(file_path)


def create_and_insert_table(database, table_schema, table_name, columns, data):
    """Create a table and insert data."""
    database.create_table(table_schema=table_schema, table_name=table_name, columns=columns)
    database.insert_values(
        data=data, table_schema=table_schema, table_name=table_name, columns=", ".join(columns.keys())
    )


def main():
    # Initialize database connection
    database = Mysql(**DB_CONFIG)

    try:
        initialize_database(database)

        # Load and insert stock symbols data
        stock_symbols_df = load_data(os.path.join(CUR_DIR, "data/company-stocks.csv"))
        stock_symbols_columns = {"ticker_symbol": "varchar(20)", "stock_name": "varchar(20)"}
        create_and_insert_table(
            database,
            table_schema="stock",
            table_name="stock_symbols",
            columns=stock_symbols_columns,
            data=stock_symbols_df,
        )

        # Load and insert stock values data
        stock_values_df = load_data(os.path.join(CUR_DIR, "data/company-stocks-history.csv"))
        stock_values_columns = {
            "ticker_symbol": "varchar(20)",
            "day_date": "timestamp",
            "close_value": "float",
            "volume": "bigint",
            "open_value": "float",
            "high_value": "float",
            "low_value": "float",
        }
        create_and_insert_table(
            database,
            table_schema="stock",
            table_name="stock_values",
            columns=stock_values_columns,
            data=stock_values_df,
        )

        print("Data is ready!")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        database.close_connection()


if __name__ == "__main__":
    main()
