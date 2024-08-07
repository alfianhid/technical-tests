import os
from datetime import datetime

import pandas as pd
import psycopg2
from mysql import connector
from pandas import DataFrame

CUR_DIR = os.path.abspath(os.path.dirname(__file__))


class Database:
    def __init__(self, database_type: str, host: str, port: int, db_name: str, user_name: str, password: str):
        self.host = host
        self.port = port
        self.db_name = db_name
        self._user_name = user_name
        self._password = password
        self.database_type = database_type
        self.conn = self._establish_connection()
        if self.conn is None:
            raise Exception("Failed to establish a database connection")
        self.cursor = self.conn.cursor()

    def _establish_connection(self):
        try:
            if self.database_type == "postgresql":
                conn = psycopg2.connect(
                    host=self.host, port=self.port, dbname=self.db_name, user=self._user_name, password=self._password
                )
                conn.set_session(autocommit=True)
            elif self.database_type == "mysql":
                conn = connector.connect(
                    host=self.host, port=self.port, database=self.db_name, user=self._user_name, password=self._password
                )
                conn.autocommit = True
            print(f"Successfully connected to {self.db_name}!")
            return conn
        except Exception as e:
            print(f"Error: {e} when connecting to {self.database_type} database! "
                  f"Please check your docker container and make sure the database is running and you have given the correct information!")
            return None

    def execute_query(self, query: str, return_data=True):
        try:
            self.cursor.execute(query)
            print("Query successful")
            if return_data:
                columns = [desc[0] for desc in self.cursor.description]
                return pd.DataFrame(self.cursor.fetchall(), columns=columns)
        except Exception as e:
            self.cursor.execute("ROLLBACK")
            self._close_connection()
            print(f"Error when executing query: '{e}'")
            raise Exception("There is a problem with your query. Please control it. Marking task as failed!")

    def truncate_table(self, table_schema: str, table_name: str) -> None:
        query = f"TRUNCATE TABLE {table_schema}.{table_name}"
        print(query)
        self.execute_query(query, return_data=False)

    def create_schema(self, table_schema: str) -> None:
        query = f"CREATE SCHEMA IF NOT EXISTS {table_schema};"
        self.execute_query(query, return_data=False)
        print(query)

    def create_table(self, table_schema: str, table_name: str, columns: dict) -> None:
        column_definitions = ",\n".join([f"{name} {dtype}" for name, dtype in columns.items()])
        query = f"CREATE TABLE IF NOT EXISTS {table_schema}.{table_name} (\n{column_definitions}\n);"
        self.execute_query(query, return_data=False)
        print(query)

    def drop_schema(self, table_schema: str) -> None:
        query = f"DROP SCHEMA IF EXISTS {table_schema};"
        self.execute_query(query, return_data=False)

    def drop_table(self, table_schema: str, table_name: str) -> None:
        query = f"DROP TABLE IF EXISTS {table_schema}.{table_name};"
        self.execute_query(query, return_data=False)

    def insert_values(self, data: DataFrame, table_schema: str, table_name: str, columns: str) -> None:
        print("Columns to be inserted: ", str(columns))
        print("Columns in the data extracted: ", str(data.columns.tolist()))

        data.reset_index(drop=True, inplace=True)
        insert_str = f"INSERT INTO {table_schema}.{table_name} ({columns}) VALUES "

        values = []
        for idx1, row in data.iterrows():
            insert_row = [
                'NULL' if pd.isna(x) else
                f"'{x}'" if isinstance(x, pd.Timestamp) else
                f"'{str(x).replace("'", "''")}'" if isinstance(x, str) else
                str(x)
                for x in row
            ]
            values.append(f"({', '.join(insert_row)})")

        insert_str += ", ".join(values) + ";"

        print("SQL query is generated at", datetime.now())
        print("START OF QUERY:")
        print(insert_str[:1000])
        print("END OF QUERY:")
        print(insert_str[-1000:])
        self.execute_query(insert_str, return_data=False)
        print("Ingestion process has completed!")

    def _close_connection(self):
        self.conn.close()
        self.cursor.close()
        print("Connection is successfully terminated!")


class Mysql(Database):
    def __init__(self, host: str, port: int, db_name: str, user_name: str, password: str):
        super().__init__("mysql", host, port, db_name, user_name, password)


class Postgresql(Database):
    def __init__(self, host: str, port: int, db_name: str, user_name: str, password: str):
        super().__init__("postgresql", host, port, db_name, user_name, password)
