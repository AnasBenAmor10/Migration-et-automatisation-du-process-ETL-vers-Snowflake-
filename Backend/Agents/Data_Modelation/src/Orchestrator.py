from SnowflakeSchemaGenerator import SnowflakeSchemaGenerator
from OracleSchemaExporter import OracleSchemaExporter
import json
from DDLCreator import DDLCreator
from utils.logger import logger
import re
from OracleDDLExecutor import OracleDDLExecutor
from dotenv import load_dotenv

load_dotenv()
# Génération du modèle
exporter = OracleSchemaExporter()
try:
    exporter.connect()
    json_file = exporter.export_schema_to_json("HR")
    with open(json_file, "r") as f:
        schema = json.load(f)
finally:
    exporter.close_connection()

generator = SnowflakeSchemaGenerator()
result = generator.generate_model(schema)
print(result)

creator = DDLCreator(result)
result = creator.generate_ddl()
print(result["ddl"])

executor = OracleDDLExecutor()
try:
    executor.connect()
    executor.create_schema(recreate=True)
    ddl_script = result["ddl"]
    executor.execute_ddl(ddl_script)

except Exception as e:
    logger.error(f"Operation failed: {e}")
finally:
    executor.close()
