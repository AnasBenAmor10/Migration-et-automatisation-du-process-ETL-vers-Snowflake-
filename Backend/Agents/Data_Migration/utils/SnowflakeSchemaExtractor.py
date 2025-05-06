import os
import snowflake.connector
from snowflake.connector import DictCursor
from typing import Dict, List, Optional
from dotenv import load_dotenv

load_dotenv()


class SnowflakeSchemaExtractor:
    """
    A class to connect to Snowflake and extract schema information including:
    - Tables and their columns with details
    - Primary keys
    - Foreign keys
    """

    def __init__(self):
        """
        Initialize the connection using environment variables.
        Expected env vars:
        - SNOWFLAKE_USER
        - SNOWFLAKE_PASSWORD
        - SNOWFLAKE_ACCOUNT
        - SNOWFLAKE_WAREHOUSE (optional)
        - SNOWFLAKE_DATABASE (optional)
        - SNOWFLAKE_SCHEMA (optional)
        """
        self.conn = None
        self.connect()

    def connect(self):
        """Establish connection to Snowflake using environment variables"""
        try:
            self.conn = snowflake.connector.connect(
                user=os.getenv("SNOWFLAKE_USER"),
                password=os.getenv("SNOWFLAKE_PASSWORD"),
                account=os.getenv("SNOWFLAKE_ACCOUNT"),
                warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                database=os.getenv("SNOWFLAKE_DATABASE"),
                schema=os.getenv("SNOWFLAKE_SCHEMA"),
                role=os.getenv("SNOWFLAKE_ROLE"),
            )
            print("Successfully connected to Snowflake")
        except Exception as e:
            print(f"Error connecting to Snowflake: {e}")
            raise

    def close(self):
        """Close the connection"""
        if self.conn:
            self.conn.close()
            print("Connection closed")

    def _execute_query(self, query: str) -> List[Dict]:
        """Execute a query and return results as dictionaries"""
        try:
            cursor = self.conn.cursor(DictCursor)
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()
            return results
        except Exception as e:
            print(f"Error executing query: {e}\nQuery: {query}")
            raise

    def get_table_details(
        self, schema_name: str = "DW", database_name: str = "HR_DB"
    ) -> List[Dict]:
        """
        Get details for all tables in the specified schema
        Returns a list of dictionaries with table/column information
        """
        query = f"""
        SELECT 
            c.TABLE_NAME,
            c.COLUMN_NAME,
            c.DATA_TYPE,
            c.IS_NULLABLE,
            c.COMMENT AS COLUMN_COMMENT
        FROM INFORMATION_SCHEMA.COLUMNS c
        WHERE c.TABLE_SCHEMA = '{schema_name}'
        AND c.TABLE_CATALOG = '{database_name}' 
        ORDER BY c.TABLE_NAME, c.ORDINAL_POSITION;
        """
        return self._execute_query(query)

    def get_primary_keys(self) -> List[Dict]:
        """Get primary key information for all tables"""
        return self._execute_query("SHOW PRIMARY KEYS;")

    def get_foreign_keys(self) -> List[Dict]:
        """Get foreign key information for all tables"""
        return self._execute_query("SHOW IMPORTED KEYS;")

    def get_schema(
        self, schema_name: str = "DW", database_name: str = "HR_DB"
    ) -> Dict[str, Dict]:
        """
        Get complete schema information organized by table
        Returns a dictionary where:
        - Keys are table names
        - Values are dictionaries with:
            - 'columns': list of column details
            - 'primary_keys': list of primary key column names
            - 'foreign_keys': list of foreign key details
        """
        # Get raw data
        columns = self.get_table_details(schema_name, database_name)
        primary_keys = self.get_primary_keys()
        foreign_keys = self.get_foreign_keys()

        # Initialize schema structure
        schema: Dict[str, Dict] = {}

        # Process columns
        for column in columns:
            table_name = column["TABLE_NAME"]
            if table_name not in schema:
                schema[table_name] = {
                    "columns": [],
                    "primary_keys": [],
                    "foreign_keys": [],
                }

            schema[table_name]["columns"].append(
                {
                    "name": column["COLUMN_NAME"],
                    "type": column["DATA_TYPE"],
                    "nullable": column["IS_NULLABLE"] == "YES",
                    "comment": column["COLUMN_COMMENT"],
                }
            )

        # Process primary keys
        for pk in primary_keys:
            table_name = pk["table_name"]
            if table_name in schema:
                schema[table_name]["primary_keys"].append(pk["column_name"])

        # Process foreign keys
        for fk in foreign_keys:
            table_name = fk["fk_table_name"]
            if table_name in schema:
                schema[table_name]["foreign_keys"].append(
                    {
                        "column": fk["fk_column_name"],
                        "references": {
                            "table": fk["pk_table_name"],
                            "column": fk["pk_column_name"],
                        },
                    }
                )

        return schema
