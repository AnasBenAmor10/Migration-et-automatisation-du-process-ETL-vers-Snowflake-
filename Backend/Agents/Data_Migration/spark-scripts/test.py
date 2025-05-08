from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import col
from pyspark.sql.types import DecimalType

spark = (
    SparkSession.builder.appName("Data_Migration")
    .config("spark.jars", "/opt/bitnami/spark/jars/ojdbc11.jar")
    .config("spark.driver.extraClassPath", "/opt/bitnami/spark/jars/ojdbc11.jar")
    .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
    .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC")
    .config(
        "spark.jars",
        "/opt/bitnami/spark/jars/snowflake-jdbc-3.23.2.jar,"
        "/opt/bitnami/spark/jars/spark-snowflake_2.12-3.1.1.jar",
    )
    .config("spark.jars.packages", "net.snowflake:spark-snowflake_2.12:2.9.0-spark_3.1")
    .getOrCreate()
)
# JDBC configuration
jdbc_config = {
    "source": {
        "url": "jdbc:oracle:thin:@host.docker.internal:1521:ORCL",
        "user": "HR",
        "password": "hr",
        "driver": "oracle.jdbc.OracleDriver",
    },
}


def read_source_table(spark, table_name):
    """Read data from Oracle source table"""
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_config["source"]["url"])
        .option("dbtable", f"{table_name}")
        .option("user", jdbc_config["source"]["user"])
        .option("password", jdbc_config["source"]["password"])
        .option("driver", jdbc_config["source"]["driver"])
        .load()
    )


source_df = read_source_table(spark, "EMPLOYEES")
# 1. MAPPING DES COLONNES & NATURAL KEYS
transformed_df = source_df.select(
    F.col("EMPLOYEE_ID"),
    F.col("JOB_ID"),
    F.col("DEPARTMENT_ID"),
    F.col("HIRE_DATE"),
    F.col("SALARY"),
    F.col("COMMISSION_PCT"),
)

# 2. CONVERSION DES TYPES
transformed_df = transformed_df.withColumn(
    "COMMISSION_PCT", F.col("COMMISSION_PCT").cast(DecimalType(10, 2))
)
transformed_df = transformed_df.na.fill(0, subset=["COMMISSION_PCT"])
transformed_df.select("COMMISSION_PCT").show(n=transformed_df.count(), truncate=False)
