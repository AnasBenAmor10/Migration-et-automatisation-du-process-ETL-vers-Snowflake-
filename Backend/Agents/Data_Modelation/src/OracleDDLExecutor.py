import cx_Oracle
import os
import re
from typing import List
from utils.logger import logger


class OracleDDLExecutor:
    def __init__(self):
        self.admin_user = os.getenv("ORACLE_ADMIN_USER")
        self.admin_password = os.getenv("ORACLE_ADMIN_PASSWORD")
        self.host = os.getenv("ORACLE_HOST")
        self.port = os.getenv("ORACLE_PORT")
        self.sid = os.getenv("ORACLE_SID")
        self.target_schema = os.getenv("TAR")
        self.target_schema_password = os.getenv("PASS_TAR")
        self.connection = None
        self.cursor = None
        self._validate_env_vars()

    def _validate_env_vars(self):
        required_vars = [
            "ORACLE_ADMIN_USER",
            "ORACLE_ADMIN_PASSWORD",
            "ORACLE_HOST",
            "ORACLE_PORT",
            "ORACLE_SID",
        ]
        missing = [var for var in required_vars if not os.getenv(var)]
        if missing:
            logger.error(f"Missing environment variables: {', '.join(missing)}")
            raise ValueError("Missing required environment variables")

    def connect(self):
        try:
            dsn = cx_Oracle.makedsn(self.host, self.port, service_name=self.sid)
            self.connection = cx_Oracle.connect(
                user=self.admin_user,
                password=self.admin_password,
                dsn=dsn,
                encoding="UTF-8",
            )
            self.cursor = self.connection.cursor()
            logger.info("Connected to Oracle database")
        except cx_Oracle.DatabaseError:
            dsn = cx_Oracle.makedsn(self.host, self.port, sid=self.sid)
            self.connection = cx_Oracle.connect(
                user=self.admin_user,
                password=self.admin_password,
                dsn=dsn,
                encoding="UTF-8",
            )
            self.cursor = self.connection.cursor()
            logger.info("Connected using SID fallback")

    def _schema_exists(self):
        self.cursor.execute(
            "SELECT username FROM all_users WHERE username = UPPER(:1)",
            [self.target_schema],
        )
        return self.cursor.fetchone() is not None

    def create_schema(self, recreate=False):
        try:
            if recreate and self._schema_exists():
                logger.info(f"Dropping existing schema {self.target_schema}")
                self.cursor.execute(f"DROP USER {self.target_schema} CASCADE")

            if not self._schema_exists():
                logger.info(f"Creating schema {self.target_schema}")
                self.cursor.execute(
                    f"CREATE USER {self.target_schema} "
                    f"IDENTIFIED BY {self.target_schema_password} "
                    f"DEFAULT TABLESPACE USERS QUOTA UNLIMITED ON USERS"
                )
                self.cursor.execute(
                    f"GRANT CREATE SESSION, CREATE TABLE, CREATE SEQUENCE, "
                    f"CREATE VIEW,CREATE TRIGGER, CREATE PROCEDURE TO {self.target_schema}"
                )
                self.connection.commit()
        except cx_Oracle.DatabaseError as e:
            self.connection.rollback()
            logger.error(f"Schema creation failed: {e}")
            raise

    # def _split_ddl_statements(self, ddl_script):
    #     cleaned_ddl = re.sub(
    #         r"/\*.*?\*/", "", ddl_script, flags=re.DOTALL
    #     )  # Remove block comments
    #     cleaned_ddl = re.sub(
    #         r"--.*$", "", cleaned_ddl, flags=re.MULTILINE
    #     )  # Remove line comments
    #     cleaned_ddl = cleaned_ddl.strip(
    #         "` \n\r\t"
    #     )  # Remove the backticks et Space  inutiles start and final
    #     cleaned_ddl = re.sub(r"sql?\s*", "", cleaned_ddl, flags=re.IGNORECASE)
    #     # Supprimer tous les caractères de type slash "/"
    #     # cleaned_ddl = cleaned_ddl.replace("/", "")  # Retirer tous les slashes
    #     statements = [stmt.strip() for stmt in cleaned_ddl.split(";") if stmt.strip()]
    #     for i, stmt in enumerate(statements):
    #         # Vérifier si la déclaration contient "CREATE OR REPLACE TRIGGER"
    #         stmt_upper = stmt.upper()
    #         if "CREATE OR REPLACE TRIGGER" in stmt_upper:
    #             # Ajouter 'END;' à la fin de la déclaration
    #             statements[i] = stmt.rstrip() + " \nEND;"

    #     categorized = {
    #         "tables": [],
    #         "indexes": [],
    #         "constraints": [],
    #         "sequences": [],
    #         "triggers": [],
    #         "comments": [],
    #         "others": [],
    #     }

    #     for stmt in statements:
    #         stmt_upper = stmt.upper()
    #         if "CREATE TABLE" in stmt_upper:
    #             categorized["tables"].append(stmt)
    #         elif "CREATE INDEX" in stmt_upper or "ALTER TABLE" in stmt_upper:
    #             if "ADD CONSTRAINT" in stmt_upper:
    #                 categorized["constraints"].append(stmt)
    #             else:
    #                 categorized["indexes"].append(stmt)
    #         elif "CREATE SEQUENCE" in stmt_upper:
    #             categorized["sequences"].append(stmt)

    #         elif (
    #             "CREATE OR REPLACE TRIGGER" in stmt_upper
    #             or "CREATE TRIGGER" in stmt_upper
    #         ):
    #             categorized["triggers"].append(stmt)

    #         elif "COMMENT ON" in stmt_upper:
    #             # Nettoyage du commentaire à l’intérieur des apostrophes
    #             stmt = re.sub(
    #                 r"(IS\s*')([^']*)(')",
    #                 lambda m: f"{m.group(1)}{re.sub(r'[;<>!?]', '', m.group(2))}{m.group(3)}",
    #                 stmt,
    #                 flags=re.IGNORECASE,
    #             )
    #             categorized["comments"].append(stmt)
    #         else:
    #             categorized["others"].append(stmt)
    #     return categorized

    def _split_ddl_statements(self, sql_content):
        # Initialize the categorized dictionary
        categorized = {
            "tables": [],
            "indexes": [],
            "constraints": [],
            "sequences": [],
            "triggers": [],
            "comments": [],
        }

        # Define patterns for each section's SQL statements with their corresponding category
        patterns = [
            (r"(CREATE\s+TABLE\s+[\w_]+\s*\([^;]*?\))(?=;)", "tables"),
            (r"ALTER\s+TABLE\s+[\w_]+\s+ADD\s+CONSTRAINT\s+[^;]+(?=;)", "constraints"),
            (r"CREATE\s+SEQUENCE\s+[^;]+(?=;)", "sequences"),
            (r"CREATE\s+OR\s+REPLACE\s+TRIGGER\s+.*?;\s*(?=/)", "triggers"),
            (r"CREATE\s+INDEX\s+[^;]+(?=;)", "indexes"),
            (r"COMMENT\s+ON\s+[^;]+?IS\s+\'[^\']*\'(?=;)", "comments"),
        ]

        # Find all matches and categorize them
        for pattern, category in patterns:
            matches = re.findall(pattern, sql_content, re.DOTALL | re.MULTILINE)
            categorized[category].extend(matches)

        return categorized

    def _execute_safe(self, statements, object_type):
        for stmt in statements:
            try:
                # clean_stmt = stmt.strip().rstrip(";")
                logger.debug(f"Executing {object_type} statement: {stmt[:60]}...")
                logger.debug(f"Full statement to execute:\n{stmt}")
                self.cursor.execute(stmt)
            except cx_Oracle.DatabaseError as e:
                if "ORA-00955" in str(e):  # Name already used
                    logger.warning(f"Object already exists: {stmt[:60]}...")
                else:
                    logger.error(f"Failed to execute {object_type} statement: {e}")
                    raise

    def execute_ddl(self, ddl_script):
        try:
            logger.debug(f"Executing Session...")
            self.cursor.execute(
                f'ALTER SESSION SET CURRENT_SCHEMA = "{self.target_schema}"'
            )
            logger.debug(f"Executing Session done...")

            # Split and categorize DDL statements
            categorized = self._split_ddl_statements(ddl_script)

            # Execute in proper order
            self._execute_safe(categorized["tables"], "table")
            self._execute_safe(categorized["constraints"], "constraint")
            self._execute_safe(categorized["indexes"], "index")
            self._execute_safe(categorized["sequences"], "sequences")
            self._execute_safe(categorized["triggers"], "triggers")
            self._execute_safe(categorized["comments"], "comment")
            # self._execute_safe(categorized["others"], "other")

            self.connection.commit()
            logger.info("DDL executed successfully")
        except cx_Oracle.DatabaseError as e:
            self.connection.rollback()
            logger.error(f"DDL execution failed: {e}")
            raise

    def verify_object_exists(self, object_name, object_type="TABLE"):
        query = """
            SELECT object_name 
            FROM all_objects 
            WHERE owner = UPPER(:owner)
            AND object_name = UPPER(:name)
            AND object_type = UPPER(:type)
        """
        self.cursor.execute(
            query,
            {"owner": self.target_schema, "name": object_name, "type": object_type},
        )
        return self.cursor.fetchone() is not None

    def verify_relationship_exists(self, table_name, constraint_name=None):
        query = """
            SELECT constraint_name
            FROM all_constraints
            WHERE owner = UPPER(:owner)
            AND table_name = UPPER(:table)
            AND constraint_type = 'R'
        """
        params = {"owner": self.target_schema, "table": table_name}

        if constraint_name:
            query += " AND constraint_name = UPPER(:constraint)"
            params["constraint"] = constraint_name

        self.cursor.execute(query, params)
        return self.cursor.fetchone() is not None

    def close(self):
        try:
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()
            logger.info("Connection closed")
        except cx_Oracle.DatabaseError as e:
            logger.error(f"Error closing connection: {e}")
