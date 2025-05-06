import os
import re
from snowflake.connector import SnowflakeConnection
from typing import Dict, Optional, List, Set
import snowflake.connector
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)


class SnowflakeDDLExecutor:
    def __init__(self):
        self.connection: Optional[SnowflakeConnection] = None
        self.cursor = None
        self._existing_tables: Set[str] = set()

    def _get_connection_params(self) -> Dict:
        """Get Snowflake connection parameters from environment variables."""
        return {
            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "user": os.getenv("SNOWFLAKE_USER"),
            "password": os.getenv("SNOWFLAKE_PASSWORD"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
            "database": os.getenv("SNOWFLAKE_DATABASE"),
            "schema": os.getenv("SNOWFLAKE_SCHEMA"),
            "role": os.getenv("SNOWFLAKE_ROLE"),
        }

    def connect(self) -> None:
        """Establish connection to Snowflake."""
        try:
            self.connection = snowflake.connector.connect(
                **self._get_connection_params()
            )
            self.cursor = self.connection.cursor()
            logger.info("Successfully connected to Snowflake")
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            raise

    def ensure_schema_exists(self, schema_name: str) -> None:
        """Create schema if it doesn't exist."""
        try:
            self.cursor.execute(f"SHOW SCHEMAS LIKE '{schema_name}'")
            if not self.cursor.fetchone():
                logger.info(f"Creating schema {schema_name}")
                self.cursor.execute(f"CREATE SCHEMA {schema_name}")
            self.cursor.execute(f"USE SCHEMA {schema_name}")
            logger.info(f"Using schema: {schema_name}")
        except Exception as e:
            logger.error(f"Schema creation failed: {e}")
            raise

    def _refresh_existing_tables(self) -> None:
        """Refresh the cache of existing tables in the current schema."""
        try:
            self.cursor.execute(
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = CURRENT_SCHEMA()"
            )
            self._existing_tables = {row[0] for row in self.cursor.fetchall()}
        except Exception as e:
            logger.error(f"Failed to refresh table list: {e}")
            raise

    def _split_ddl_statements(self, sql_content: str) -> Dict[str, List[str]]:
        """Categorize DDL statements into tables, constraints, and comments."""
        # First normalize the SQL content
        sql_content = re.sub(
            r"^```sql|```$", "", sql_content, flags=re.MULTILINE | re.IGNORECASE
        )
        sql_content = re.sub(r"--.*?\n", "", sql_content)  # Remove single-line comments
        sql_content = re.sub(r"/\*.*?\*/", "", sql_content, flags=re.DOTALL)
        # print(sql_content)
        categorized = {
            "tables": [],
            "constraints": [],
            "comments": [],
        }

        # Improved patterns
        patterns = [
            # Table pattern - matches CREATE OR REPLACE TABLE with full body
            (
                r"(CREATE OR REPLACE\s+TABLE\s+[\w_]+\s*\([^;]*?\))(?=;)",
                "tables",
            ),
            # Constraint pattern - matches all ALTER TABLE statements
            (
                r"(ALTER\s+TABLE\s+(?:\w+\.)?(\w+)\s+(?:ADD\s+CONSTRAINT\s+[^;]+|ALTER\s+COLUMN\s+[^;]+|ADD\s+PRIMARY\s+KEY\s*\([^;]*\))\s*;)",
                "constraints",
            ),
            # Comment pattern - matches both table and column comments
            (
                r"(COMMENT\s+ON\s+(?:TABLE|COLUMN)\s+(?:\w+\.)?(?:\w+(?:\.\w+)?)\s+IS\s+(?:\'[^\']*\'|NULL)\s*;)",
                "comments",
            ),
        ]

        for pattern, category in patterns:
            try:
                matches = re.findall(pattern, sql_content, re.IGNORECASE)
                # For tables and constraints, we take the full match (group 0)
                # For comments, we take the full match directly
                if category in ["constraints"]:
                    categorized[category].extend([m[0] for m in matches])
                else:
                    categorized[category].extend(matches)
                logger.debug(
                    f"Found {len(categorized[category])} {category} statements"
                )
            except Exception as e:
                logger.warning(f"Error parsing {category}: {e}")
        # print(categorized["tables"])

        return categorized

    def _execute_tables(self, statements: List[str]) -> None:
        """Execute table creation statements and track created tables."""
        for stmt in statements:
            try:
                self.cursor.execute(stmt)
                # Extract table name from CREATE TABLE statement
                table_name = re.search(
                    r"CREATE\s+OR\s+REPLACE\s+TABLE\s+(?:\w+\.)?(\w+)",
                    stmt,
                    re.IGNORECASE,
                ).group(1)
                self._existing_tables.add(table_name)
                logger.info(f"Created table: {table_name}")
            except Exception as e:
                logger.error(f"Failed to create table: {e}\nStatement: {stmt[:200]}...")
                raise

    def _execute_constraints(self, statements: List[str]) -> None:
        """Execute constraint statements with validation."""
        for stmt in statements:
            try:
                # Extract table name from ALTER TABLE statement
                table_name = re.search(
                    r"ALTER\s+TABLE\s+(?:\w+\.)?(\w+)", stmt, re.IGNORECASE
                ).group(1)
                if table_name not in self._existing_tables:
                    raise ValueError(
                        f"Cannot add constraints - table {table_name} does not exist"
                    )

                self.cursor.execute(stmt)
                logger.debug(f"Added constraint to {table_name}")
            except Exception as e:
                logger.error(
                    f"Failed to add constraint: {e}\nStatement: {stmt[:200]}..."
                )
                raise

    def _execute_comments(self, statements: List[str]) -> None:
        """Execute comment statements."""
        for stmt in statements:
            if stmt.strip().upper().endswith("IS NULL;"):
                continue

            try:
                self.cursor.execute(stmt)
                logger.debug(f"Added comment: {stmt[:200]}...")
            except Exception as e:
                logger.warning(
                    f"Failed to add comment (continuing anyway): {e}\nStatement: {stmt[:200]}..."
                )

    def execute(self, ddl_script: str, schema_name: Optional[str] = None) -> Dict:
        """
        Execute full DDL workflow with proper statement ordering.

        Args:
            ddl_script: SQL DDL script to execute
            schema_name: Target schema (defaults to env var)

        Returns:
            Dictionary with execution status
        """
        if not ddl_script:
            return {"error": "No DDL script provided"}

        schema_name = schema_name or os.getenv("SNOWFLAKE_SCHEMA")
        if not schema_name:
            return {"error": "No schema name provided"}

        try:
            self.connect()
            self.ensure_schema_exists(schema_name)
            self._refresh_existing_tables()

            # Categorize statements
            categorized = self._split_ddl_statements(ddl_script)
            logger.info(
                f"Executing {len(categorized['tables'])} tables, "
                f"{len(categorized['constraints'])} constraints, "
                f"{len(categorized['comments'])} comments"
            )

            # Execute in proper order
            self._execute_tables(categorized["tables"])
            self._refresh_existing_tables()
            self._execute_constraints(categorized["constraints"])
            self._execute_comments(categorized["comments"])

            return {
                "success": True,
                "message": f"DDL executed successfully in schema {schema_name}",
                "stats": {
                    "tables_created": len(categorized["tables"]),
                    "constraints_added": len(categorized["constraints"]),
                    "comments_added": len(categorized["comments"]),
                },
            }
        except Exception as e:
            logger.error(f"DDL execution failed: {e}")
            return {
                "success": False,
                "error": str(e),
                "message": "DDL execution failed",
            }
        finally:
            self.close()

    def close(self) -> None:
        """Close connection resources."""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("Connection closed")
