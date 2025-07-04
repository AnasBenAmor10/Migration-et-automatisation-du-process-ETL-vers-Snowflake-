from pyspark.sql import SparkSession

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


SF_OPTS = {
    "sfURL": "RTXXVIG-RZ25842.snowflakecomputing.com",
    "sfUser": "ANASBENAMOR",
    "sfPassword": "Benamor060401*",
    "sfDatabase": "HR_DB",
    "sfSchema": "DW",
    "sfWarehouse": "COMPUTE_WH",
    "sfRole": "ACCOUNTADMIN",
    "column_mismatch_behavior": "ignore",
    "column_mapping": "name",
}

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


def read_from_snowflake(spark, table_name):
    """Read data from Oracle source table"""
    return (
        spark.read.format("net.snowflake.spark.snowflake")
        .options(**SF_OPTS)
        .option("dbtable", table_name)
        .load()
    )


def write_to_snowflake(df, table_name):
    df.write.format("net.snowflake.spark.snowflake").options(**SF_OPTS).option(
        "dbtable", table_name
    ).option("sfCompress", "on").option(
        "error_on_column_count_mismatch", "false"
    ).option(
        "columnMapping", "name"
    ).mode(
        "append"
    ).save()


# Transformations Spark générées automatiquement
# Contient les transformations pour toutes les tables


################################################################################
# Transformation pour la table D_REGION
################################################################################

from pyspark.sql import functions as F
from pyspark.sql.types import *


def transform_D_REGION(source_df, dimension_dfs):
    """
    Transforms source data for D_REGION according to mapping specs
    Args:
        source_df: DataFrame containing source data
        dimension_dfs: Dict of dimension DataFrames {'DIM1': df1, 'DIM2': df2}
    Returns:
        Transformed DataFrame matching target schema
    """

    # 1. MAPPING DES COLONNES
    transformed_df = source_df.select(
        F.col("REGION_ID").alias("REGION_ID"), F.col("REGION_NAME").alias("REGION_NAME")
    )

    # 2. CONVERSION DES TYPES (No type conversion needed as per mapping)

    # 3. NULL HANDLING POLICY
    transformed_df = transformed_df.na.fill("N/A", subset=["REGION_NAME"])

    # 4. CONTROLES QUALITE
    if transformed_df.filter(F.col("REGION_ID").isNull()).count() > 0:
        raise ValueError("REGION_ID cannot be null")

    # 5. DIMENSION JOINS (Not required as per mapping)

    # 6. Ajout des métadonnées techniques
    transformed_df = transformed_df.withColumn("DT_INSERT", F.current_timestamp())

    # 7. SELECTION FINALE
    final_columns = [
        "REGION_ID",
        "REGION_NAME",
        "DT_INSERT",
    ]
    valid_df = transformed_df.select(final_columns)

    return valid_df


################################################################################
# Transformation pour la table D_COUNTRY
################################################################################

from pyspark.sql import functions as F
from pyspark.sql.types import *


def transform_D_COUNTRY(source_df, dimension_dfs):
    """
    Transforms source data for D_COUNTRY according to mapping specs
    Args:
        source_df: DataFrame containing source data
        dimension_dfs: Dict of dimension DataFrames {'D_REGION': df_region}
    Returns:
        Transformed DataFrame matching target schema
    """

    # 1. MAPPING DES COLONNES
    transformed_df = source_df.select(
        F.col("COUNTRY_ID"), F.col("COUNTRY_NAME"), F.col("REGION_ID")
    )

    # 2. CONVERSION DES TYPES (None needed as per the mapping)

    # 3. NULL HANDLING POLICY
    transformed_df = transformed_df.na.fill("N/A", subset=["COUNTRY_NAME"])

    # 4. CONTROLES QUALITE
    if transformed_df.filter(F.col("COUNTRY_ID").isNull()).count() > 0:
        raise ValueError("COUNTRY_ID cannot be null")

    # 5. DIMENSION JOINS
    dim_region_df = dimension_dfs["D_REGION"]
    transformed_df = transformed_df.join(
        dim_region_df.select("REGION_ID", "SK_REGION_ID"),
        transformed_df["REGION_ID"] == dim_region_df["REGION_ID"],
        "left",
    ).drop("REGION_ID")

    transformed_df = transformed_df.withColumn(
        "SK_REGION_ID", F.coalesce(F.col("SK_REGION_ID"), F.lit(-1))
    )

    # 6. Ajout des métadonnées techniques
    transformed_df = transformed_df.withColumn("DT_INSERT", F.current_timestamp())

    # 7. SELECTION FINALE
    final_columns = [
        "COUNTRY_ID",
        "COUNTRY_NAME",
        "SK_REGION_ID",
        "DT_INSERT",
    ]
    valid_df = transformed_df.select(final_columns)

    return valid_df


################################################################################
# Transformation pour la table D_LOCATION
################################################################################

from pyspark.sql import functions as F
from pyspark.sql.types import *


def transform_D_LOCATION(source_df, dimension_dfs):
    """
    Transforms source data for D_LOCATION according to mapping specs
    Args:
        source_df: DataFrame containing source data
        dimension_dfs: Dict of dimension DataFrames {'D_COUNTRY': df_country}
    Returns:
        Transformed DataFrame matching target schema
    """

    # 1. MAPPING DES COLONNES
    transformed_df = source_df.select(
        F.col("LOCATION_ID"),
        F.col("STREET_ADDRESS"),
        F.col("POSTAL_CODE"),
        F.col("CITY"),
        F.col("STATE_PROVINCE"),
        F.col("COUNTRY_ID"),  # Keep for join
    )

    # 2. CONVERSION DES TYPES (No explicit type casting needed as all source columns are strings)

    # 3. NULL HANDLING POLICY
    transformed_df = transformed_df.na.fill(
        "N/A", subset=["STREET_ADDRESS", "POSTAL_CODE", "CITY", "STATE_PROVINCE"]
    )

    # 4. CONTROLES QUALITE
    if transformed_df.filter(F.col("LOCATION_ID").isNull()).count() > 0:
        raise ValueError("LOCATION_ID cannot be null")

    # 5. DIMENSION JOINS
    if "D_COUNTRY" in dimension_dfs:
        d_country_df = dimension_dfs["D_COUNTRY"]
        transformed_df = (
            transformed_df.join(
                d_country_df.select("COUNTRY_ID", "SK_COUNTRY_ID"),
                transformed_df["COUNTRY_ID"] == d_country_df["COUNTRY_ID"],
                "left",
            )
            .withColumn("SK_COUNTRY_ID", F.coalesce(F.col("SK_COUNTRY_ID"), F.lit(-1)))
            .drop("COUNTRY_ID")
        )
    else:
        transformed_df = transformed_df.withColumn(
            "SK_COUNTRY_ID", F.lit(-1)
        )  # Handle missing dimension

    # 6. Ajout des métadonnées techniques
    transformed_df = transformed_df.withColumn("DT_INSERT", F.current_timestamp())

    # 7. SELECTION FINALE
    final_columns = [
        "LOCATION_ID",
        "STREET_ADDRESS",
        "POSTAL_CODE",
        "CITY",
        "STATE_PROVINCE",
        "SK_COUNTRY_ID",
        "DT_INSERT",
    ]
    valid_df = transformed_df.select(final_columns)

    return valid_df


################################################################################
# Transformation pour la table D_JOB
################################################################################

from pyspark.sql import functions as F
from pyspark.sql.types import *


def transform_D_JOB(source_df, dimension_dfs):
    """
    Transforms source data for D_JOB according to mapping specs
    Args:
        source_df: DataFrame containing source data
        dimension_dfs: Dict of dimension DataFrames (keys: dimension table names)
    Returns:
        Transformed DataFrame matching target schema
    """

    # 1. MAPPING DES COLONNES
    transformed_df = source_df.select(
        F.col("JOB_ID").alias("JOB_ID"),
        F.col("JOB_TITLE").alias("JOB_TITLE"),
        F.col("MIN_SALARY").alias("MIN_SALARY"),
        F.col("MAX_SALARY").alias("MAX_SALARY"),
    )

    # 2. CONVERSION DES TYPES
    transformed_df = transformed_df.withColumn(
        "MIN_SALARY", F.col("MIN_SALARY").cast(DecimalType(10, 2))
    ).withColumn("MAX_SALARY", F.col("MAX_SALARY").cast(DecimalType(10, 2)))

    # 3. NULL HANDLING POLICY
    transformed_df = transformed_df.na.fill("N/A", subset=["JOB_TITLE"])
    transformed_df = transformed_df.na.fill(0, subset=["MIN_SALARY", "MAX_SALARY"])

    # 4. CONTROLES QUALITE
    if transformed_df.filter(F.col("JOB_ID").isNull()).count() > 0:
        raise ValueError("JOB_ID cannot be null")

    # 5. Ajout des métadonnées techniques
    valid_df = transformed_df.withColumn("DT_INSERT", F.current_timestamp())

    # 6. SELECTION FINALE
    final_columns = [
        "JOB_ID",
        "JOB_TITLE",
        "MIN_SALARY",
        "MAX_SALARY",
        "DT_INSERT",
    ]
    valid_df = valid_df.select(final_columns)

    return valid_df


################################################################################
# Transformation pour la table D_EMPLOYEE
################################################################################

from pyspark.sql import functions as F
from pyspark.sql.types import *


def transform_D_EMPLOYEE(source_df, dimension_dfs):
    """
    Transforms source data for D_EMPLOYEE according to mapping specs
    Args:
        source_df: DataFrame containing source data
        dimension_dfs: Dict of dimension DataFrames {'D_EMPLOYEE': df1}
    Returns:
        Transformed DataFrame matching target schema
    """

    # 1. MAPPING DES COLONNES
    transformed_df = source_df.select(
        F.col("EMPLOYEE_ID"),
        F.col("FIRST_NAME"),
        F.col("LAST_NAME"),
        F.col("EMAIL"),
        F.col("PHONE_NUMBER"),
        F.col("MANAGER_ID"),
    )

    # 2. CONVERSION DES TYPES (No type conversion needed as all target columns are String)

    # 3. NULL HANDLING POLICY
    transformed_df = transformed_df.na.fill(
        "N/A", subset=["FIRST_NAME", "LAST_NAME", "EMAIL", "PHONE_NUMBER"]
    )

    # 4. CONTROLES QUALITE
    if transformed_df.filter(F.col("EMPLOYEE_ID").isNull()).count() > 0:
        raise ValueError("EMPLOYEE_ID cannot be null")

    # 5. DIMENSION JOINS (None needed as no SK_ columns are present and MANAGER_ID is a natural key)

    # 6. Ajout des métadonnées techniques
    transformed_df = transformed_df.withColumn("DT_INSERT", F.current_timestamp())

    # 7. SELECTION FINALE
    final_columns = [
        "EMPLOYEE_ID",
        "FIRST_NAME",
        "LAST_NAME",
        "EMAIL",
        "PHONE_NUMBER",
        "MANAGER_ID",
        "DT_INSERT",
    ]
    transformed_df = transformed_df.select(final_columns)

    return transformed_df


################################################################################
# Transformation pour la table D_DEPARTMENT
################################################################################

from pyspark.sql import functions as F
from pyspark.sql.types import *


def transform_D_DEPARTMENT(source_df, dimension_dfs):
    """
    Transforms source data for D_DEPARTMENT according to mapping specs
    Args:
        source_df: DataFrame containing source data
        dimension_dfs: Dict of dimension DataFrames {'D_LOCATION': df_location}
    Returns:
        Transformed DataFrame matching target schema
    """

    # 1. MAPPING DES COLONNES
    transformed_df = source_df.select(
        F.col("DEPARTMENT_ID"),
        F.col("DEPARTMENT_NAME"),
        F.col("LOCATION_ID"),
        F.col("MANAGER_ID"),
    )

    # 2. CONVERSION DES TYPES (None needed as per mapping)

    # 3. NULL HANDLING POLICY
    transformed_df = transformed_df.na.fill("N/A", subset=["DEPARTMENT_NAME"])

    # 4. CONTROLES QUALITE
    if transformed_df.filter(F.col("DEPARTMENT_ID").isNull()).count() > 0:
        raise ValueError("DEPARTMENT_ID cannot be null")

    # 5. DIMENSION JOINS
    df_location = dimension_dfs["D_LOCATION"]
    transformed_df = transformed_df.join(
        df_location.select("LOCATION_ID", "SK_LOCATION_ID"),
        transformed_df["LOCATION_ID"] == df_location["LOCATION_ID"],
        "left",
    ).drop("LOCATION_ID")

    transformed_df = transformed_df.withColumn(
        "SK_LOCATION_ID", F.coalesce(F.col("SK_LOCATION_ID"), F.lit(-1))
    )

    # 6. Ajout des métadonnées techniques
    transformed_df = transformed_df.withColumn("DT_INSERT", F.current_timestamp())

    # 7. SELECTION FINALE
    final_columns = [
        "DEPARTMENT_ID",
        "DEPARTMENT_NAME",
        "MANAGER_ID",
        "SK_LOCATION_ID",
        "DT_INSERT",
    ]
    valid_df = transformed_df.select(final_columns)

    return valid_df


################################################################################
# Transformation pour la table D_DATE
################################################################################

from pyspark.sql import functions as F
from pyspark.sql.types import *


def transform_D_DATE(source_df, dimension_dfs):
    """
    Transforms source data for D_DATE according to mapping specs
    Args:
        source_df: DataFrame containing source data
        dimension_dfs: Dict of dimension DataFrames {'DIM1': df1, 'DIM2': df2}
    Returns:
        Transformed DataFrame matching target schema
    """

    # 1. MAPPING DES COLONNES
    transformed_df = source_df.select(
        F.col("HIRE_DATE").alias("DATE_VALUE"),
        F.col("HIRE_DATE").alias("HIRE_DATE_TEMP"),
    )

    # 2. CONVERSION DES TYPES & DATE COMPONENTS
    transformed_df = (
        transformed_df.withColumn("DATE_VALUE", F.to_date("DATE_VALUE"))
        .withColumn("DAY", F.dayofmonth("HIRE_DATE_TEMP"))
        .withColumn("MONTH", F.month("HIRE_DATE_TEMP"))
        .withColumn("YEAR", F.year("HIRE_DATE_TEMP"))
        .drop("HIRE_DATE_TEMP")
    )

    # 3. NULL HANDLING POLICY
    transformed_df = transformed_df.na.fill("1900-01-01", subset=["DATE_VALUE"])

    # 4. CONTROLES QUALITE
    if transformed_df.filter(F.col("DATE_VALUE").isNull()).count() > 0:
        raise ValueError("Null values in DATE_VALUE")

    # 5. Ajout des métadonnées techniques
    valid_df = transformed_df.withColumn("DT_INSERT", F.current_timestamp())

    # 6. SELECTION FINALE
    final_columns = [
        "DATE_VALUE",
        "DAY",
        "MONTH",
        "YEAR",
        "DT_INSERT",
    ]
    valid_df = valid_df.select(final_columns)

    return valid_df


################################################################################
# Transformation pour la table F_EMPLOYEE_SALARY
################################################################################

from pyspark.sql import functions as F
from pyspark.sql.types import *


def transform_F_EMPLOYEE_SALARY(source_df, dimension_dfs):
    """
    Transforms source data for F_EMPLOYEE_SALARY according to mapping specs
    Args:
        source_df: DataFrame containing source data
        dimension_dfs: Dict of dimension DataFrames {'D_EMPLOYEE': df1, 'D_JOB': df2, 'D_DEPARTMENT': df3, 'D_DATE': df4}
    Returns:
        Transformed DataFrame matching target schema
    """

    # 1. MAPPING DES COLONNES & NATURAL KEYS
    transformed_df = source_df.select(
        F.col("EMPLOYEE_ID"),
        F.col("JOB_ID"),
        F.col("DEPARTMENT_ID"),
        F.col("HIRE_DATE"),
        F.col("SALARY"),
        F.col("COMMISSION_PCT"),
    )

    # 2. DIMENSION JOINS
    # Join with D_EMPLOYEE
    d_employee_df = dimension_dfs["D_EMPLOYEE"]
    transformed_df = (
        transformed_df.join(
            d_employee_df.select("EMPLOYEE_ID", "SK_EMPLOYEE_ID"),
            transformed_df["EMPLOYEE_ID"] == d_employee_df["EMPLOYEE_ID"],
            "left",
        )
        .withColumn("SK_EMPLOYEE_ID", F.coalesce(F.col("SK_EMPLOYEE_ID"), F.lit(-1)))
        .drop("EMPLOYEE_ID")
    )

    # Join with D_JOB
    d_job_df = dimension_dfs["D_JOB"]
    transformed_df = (
        transformed_df.join(
            d_job_df.select("JOB_ID", "SK_JOB_ID"),
            transformed_df["JOB_ID"] == d_job_df["JOB_ID"],
            "left",
        )
        .withColumn("SK_JOB_ID", F.coalesce(F.col("SK_JOB_ID"), F.lit(-1)))
        .drop("JOB_ID")
    )

    # Join with D_DEPARTMENT
    d_department_df = dimension_dfs["D_DEPARTMENT"]
    transformed_df = (
        transformed_df.join(
            d_department_df.select("DEPARTMENT_ID", "SK_DEPARTMENT_ID"),
            transformed_df["DEPARTMENT_ID"] == d_department_df["DEPARTMENT_ID"],
            "left",
        )
        .withColumn(
            "SK_DEPARTMENT_ID", F.coalesce(F.col("SK_DEPARTMENT_ID"), F.lit(-1))
        )
        .drop("DEPARTMENT_ID")
    )

    # Join with D_DATE
    d_date_df = dimension_dfs["D_DATE"]
    transformed_df = (
        transformed_df.join(
            d_date_df.select("DATE_VALUE", "SK_DATE_ID"),
            F.to_date(transformed_df["HIRE_DATE"], "yyyy-MM-dd")
            == F.to_date(d_date_df["DATE_VALUE"], "yyyy-MM-dd"),
            "left",
        )
        .withColumn("SK_DATE_ID", F.coalesce(F.col("SK_DATE_ID"), F.lit(-1)))
        .drop("HIRE_DATE")
    )

    # 3. CONVERSION DES TYPES & NULL HANDLING
    transformed_df = transformed_df.withColumn(
        "SALARY",
        F.when(F.col("SALARY").isNull(), 0.0)
        .otherwise(F.col("SALARY"))
        .cast(DecimalType(10, 2)),
    ).withColumn(
        "COMMISSION_PCT",
        F.when(F.col("COMMISSION_PCT").isNull(), 0.0)
        .otherwise(F.col("COMMISSION_PCT"))
        .cast(DecimalType(10, 2)),
    )

    # 4. CONTROLES QUALITE (Foreign Key Checks - Example)
    if transformed_df.filter(F.col("SK_EMPLOYEE_ID") == -1).count() > 0:
        raise ValueError("Invalid SK_EMPLOYEE_ID found")
    if transformed_df.filter(F.col("SK_JOB_ID") == -1).count() > 0:
        raise ValueError("Invalid SK_JOB_ID found")
    if transformed_df.filter(F.col("SK_DEPARTMENT_ID") == -1).count() > 0:
        raise ValueError("Invalid SK_DEPARTMENT_ID found")
    if transformed_df.filter(F.col("SK_DATE_ID") == -1).count() > 0:
        raise ValueError("Invalid SK_DATE_ID found")

    # 5. Ajout des métadonnées techniques
    transformed_df = transformed_df.withColumn("DT_INSERT", F.current_timestamp())

    # 6. SELECTION FINALE
    final_columns = [
        "SK_EMPLOYEE_ID",
        "SK_JOB_ID",
        "SK_DEPARTMENT_ID",
        "SK_DATE_ID",
        "SALARY",
        "COMMISSION_PCT",
        "DT_INSERT",
    ]
    valid_df = transformed_df.select(final_columns)

    return valid_df


# Main transformation function
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main(spark):
    """
    Main function to orchestrate the data migration process.

    Args:
        spark: SparkSession object.
    """
    dimension_dfs = {}

    try:
        # Read source tables once
        regions_source_df = read_source_table(spark, "REGIONS")
        countries_source_df = read_source_table(spark, "COUNTRIES")
        locations_source_df = read_source_table(spark, "LOCATIONS")
        jobs_source_df = read_source_table(spark, "JOBS")
        employees_source_df = read_source_table(spark, "EMPLOYEES")
        departments_source_df = read_source_table(spark, "DEPARTMENTS")
        employees_source_df_date = read_source_table(spark, "EMPLOYEES")

        # D_REGION
        try:
            logger.info("Starting processing for D_REGION...")
            valid_d_region_df = transform_D_REGION(regions_source_df, dimension_dfs)
            write_to_snowflake(valid_d_region_df, "D_REGION")
            logger.info("Completed D_REGION successfully")
        except Exception as e:
            logger.error("Failed processing D_REGION: " + str(e))
            raise

        # D_COUNTRY
        try:
            logger.info("Starting processing for D_COUNTRY...")
            if "D_REGION" not in dimension_dfs:
                dimension_dfs["D_REGION"] = read_from_snowflake(spark, "D_REGION")
            valid_d_country_df = transform_D_COUNTRY(countries_source_df, dimension_dfs)
            write_to_snowflake(valid_d_country_df, "D_COUNTRY")
            logger.info("Completed D_COUNTRY successfully")
        except Exception as e:
            logger.error("Failed processing D_COUNTRY: " + str(e))
            raise

        # D_LOCATION
        try:
            logger.info("Starting processing for D_LOCATION...")
            if "D_COUNTRY" not in dimension_dfs:
                dimension_dfs["D_COUNTRY"] = read_from_snowflake(spark, "D_COUNTRY")
            valid_d_location_df = transform_D_LOCATION(
                locations_source_df, dimension_dfs
            )
            write_to_snowflake(valid_d_location_df, "D_LOCATION")
            logger.info("Completed D_LOCATION successfully")
        except Exception as e:
            logger.error("Failed processing D_LOCATION: " + str(e))
            raise

        # D_JOB
        try:
            logger.info("Starting processing for D_JOB...")
            valid_d_job_df = transform_D_JOB(jobs_source_df, dimension_dfs)
            write_to_snowflake(valid_d_job_df, "D_JOB")
            logger.info("Completed D_JOB successfully")
        except Exception as e:
            logger.error("Failed processing D_JOB: " + str(e))
            raise

        # D_DEPARTMENT
        try:
            logger.info("Starting processing for D_DEPARTMENT...")
            if "D_LOCATION" not in dimension_dfs:
                dimension_dfs["D_LOCATION"] = read_from_snowflake(spark, "D_LOCATION")
            valid_d_department_df = transform_D_DEPARTMENT(
                departments_source_df, dimension_dfs
            )
            write_to_snowflake(valid_d_department_df, "D_DEPARTMENT")
            logger.info("Completed D_DEPARTMENT successfully")
        except Exception as e:
            logger.error("Failed processing D_DEPARTMENT: " + str(e))
            raise

        # D_EMPLOYEE
        try:
            logger.info("Starting processing for D_EMPLOYEE...")
            valid_d_employee_df = transform_D_EMPLOYEE(
                employees_source_df, dimension_dfs
            )
            write_to_snowflake(valid_d_employee_df, "D_EMPLOYEE")
            logger.info("Completed D_EMPLOYEE successfully")
        except Exception as e:
            logger.error("Failed processing D_EMPLOYEE: " + str(e))
            raise

        # D_DATE
        try:
            logger.info("Starting processing for D_DATE...")
            valid_d_date_df = transform_D_DATE(employees_source_df_date, dimension_dfs)
            write_to_snowflake(valid_d_date_df, "D_DATE")
            logger.info("Completed D_DATE successfully")
        except Exception as e:
            logger.error("Failed processing D_DATE: " + str(e))
            raise

        # F_EMPLOYEE_SALARY
        try:
            logger.info("Starting processing for F_EMPLOYEE_SALARY...")
            if "D_EMPLOYEE" not in dimension_dfs:
                dimension_dfs["D_EMPLOYEE"] = read_from_snowflake(spark, "D_EMPLOYEE")
            if "D_JOB" not in dimension_dfs:
                dimension_dfs["D_JOB"] = read_from_snowflake(spark, "D_JOB")
            if "D_DEPARTMENT" not in dimension_dfs:
                dimension_dfs["D_DEPARTMENT"] = read_from_snowflake(
                    spark, "D_DEPARTMENT"
                )
            if "D_DATE" not in dimension_dfs:
                dimension_dfs["D_DATE"] = read_from_snowflake(spark, "D_DATE")
            f_employee_salary_source_df = read_source_table(spark, "EMPLOYEES")
            valid_f_employee_salary_df = transform_F_EMPLOYEE_SALARY(
                f_employee_salary_source_df, dimension_dfs
            )
            write_to_snowflake(valid_f_employee_salary_df, "F_EMPLOYEE_SALARY")
            logger.info("Completed F_EMPLOYEE_SALARY successfully")
        except Exception as e:
            logger.error("Failed processing F_EMPLOYEE_SALARY: " + str(e))
            raise

    except Exception as e:
        logger.error("An unexpected error occurred: " + str(e))
        raise


if __name__ == "__main__":
    main(spark)
