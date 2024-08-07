import os

import pandas as pd
from helpers.connections import Mysql

# Constants
CUR_DIR = os.path.abspath(os.path.dirname(__file__))
DB_CONFIG = {
    "host": "localhost",
    "port": os.environ.get("MYSQL_PORT"),
    "db_name": os.environ.get("MYSQL_DB"),
    "user_name": os.environ.get("MYSQL_USER"),
    "password": os.environ.get("MYSQL_PASSWORD"),
}

database = Mysql(
    host=DB_CONFIG["host"],
    port=DB_CONFIG["port"],
    db_name=DB_CONFIG["db_name"],
    user_name=DB_CONFIG["user_name"],
    password=DB_CONFIG["password"],
)

database.drop_table(table_schema="stock", table_name="stock_symbols")
database.drop_table(table_schema="stock", table_name="stock_values")
database.drop_schema(table_schema="stock")

database.create_schema("stock")

# Reads the data, creates the table and inserts values to stock_symbols
stock_symbols_df = pd.read_csv(CUR_DIR + "/data/company-stocks.csv")
database.create_table(
    table_schema="stock",
    table_name="stock_symbols",
    columns={"ticker_symbol": "varchar(20)", "stock_name": "varchar(20)"},
)
database.insert_values(
    data=stock_symbols_df, table_schema="stock", table_name="stock_symbols", columns="ticker_symbol, stock_name"
)

# Reads the data, creates the table and inserts values to stock_values
stock_values_df = pd.read_csv(CUR_DIR + "/data/company-stocks-history.csv")
database.create_table(
    table_schema="stock",
    table_name="stock_values",
    columns={
        "ticker_symbol": "varchar(20)",
        "day_date": "timestamp",
        "close_value": "float",
        "volume": "bigint",
        "open_value": "float",
        "high_value": "float",
        "low_value": "float",
    },
)
database.insert_values(
    data=stock_values_df,
    table_schema="stock",
    table_name="stock_values",
    columns="ticker_symbol, day_date, close_value, volume, open_value, high_value, low_value",
)

print("Data is ready!")
database.close_connection()
