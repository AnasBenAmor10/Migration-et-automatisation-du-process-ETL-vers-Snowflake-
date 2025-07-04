import logging
import os
from dotenv import load_dotenv
import re
from langchain_google_genai import ChatGoogleGenerativeAI
from utils.Prompt import *
import smtplib
from email.message import EmailMessage
from langchain_groq import ChatGroq
from langchain.schema import HumanMessage, SystemMessage
import re
from collections import defaultdict
from typing import Dict, Set

# Chargement des variables d'environnement
load_dotenv()

# Configuration du logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def schema_comparisation(oracle_schema: Dict, snowflake_schema: Dict) -> bool:
    """
    Compare Oracle and Snowflake schemas.
    Returns True if schemas are identical, False otherwise.
    """
    snowflake_data = Snowflake_schema_info(snowflake_schema)
    oracle_data = extract_oracle_schema(oracle_schema)
    return snowflake_data == oracle_data


def mapping_generation(schema_source, schema_target):
    llm = ChatGoogleGenerativeAI(
        model="gemini-2.0-flash-lite",
        google_api_key=os.getenv("GEMINI_API_KEY"),
        temperature=0,
        max_tokens=None,
        timeout=None,
        max_retries=3,
    )
    formatted_prompt = mapping_prompt.format(
        source_schema=schema_source, target_schema=schema_target
    )

    response = llm.invoke(formatted_prompt)
    return response.content


def formulated_mapping(mapping):
    pattern = r"## TRANSFORMATION FOR\s+(\w+)(.*?)(?=## TRANSFORMATION FOR|$)"

    # Find all matches with re.DOTALL to handle multi-line content
    matches = re.finditer(pattern, mapping, re.DOTALL)

    # Initialize dictionary to store results
    transformation_dict = {}

    for match in matches:
        table_name = match.group(1)  # Table name (e.g., D_REGION)
        content = match.group(2).strip()  # Full content after table name

        # Find the start of "1. Target Table" to trim content
        target_start = content.find("1. Target Table")
        if target_start != -1:
            content = content[target_start:]  # Keep only from "1. Target Table" onward

        # Add to dictionary
        transformation_dict[table_name] = content

    return transformation_dict


def get_processing_order(mapping):

    pattern = r"## TRANSFORMATION FOR\s+(\w+)"

    # Find all matches in the text
    matches = re.findall(pattern, mapping)

    # Return the list of table names in order of appearance
    return matches


def generate_table_code(table_name, table_mapping):

    llm = ChatGoogleGenerativeAI(
        model="gemini-2.0-flash-lite",
        google_api_key=os.getenv("GEMINI_API_KEY"),
        temperature=0,
        max_tokens=None,
        timeout=None,
        max_retries=2,
    )
    formatted_prompt = SparkPrompt.format(
        table_name=table_name, table_mapping=table_mapping
    )

    response = llm.invoke(formatted_prompt)
    return response.content


def get_tables(mapping):
    target = r"## TRANSFORMATION FOR\s+(\w+)"
    source = r"Source Tables:\s*(.*)"
    # Find all matches in the text
    target_table = re.findall(target, mapping)
    source_table = re.findall(source, mapping)
    # Return the list of table names in order of appearance
    return source_table, target_table


def generate_main(code, input_tables, target_tables):
    model = ChatGoogleGenerativeAI(
        model="gemini-2.0-flash-lite",
        google_api_key=os.getenv("GEMINI_API_KEY2"),
        temperature=0,
        max_tokens=None,
        timeout=None,
        max_retries=2,
    )

    prompt = main_prompt(code, input_tables, target_tables)
    response = model.invoke(prompt)

    return response.content


def write_main(code):
    # Nom du fichier de sortie
    output_file = "spark-scripts/spark_transformations.py"
    import textwrap

    header_code = textwrap.dedent(
        """\
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
            \"\"\"Read data from Oracle source table\"\"\"
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
            \"\"\"Read data from Oracle source table\"\"\"
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
    """
    )

    # Écrire tous les codes dans un seul fichier
    with open(output_file, "a", encoding="utf-8") as f:
        # Écrire l'en-tête
        f.write(header_code)

        # Nettoyer le code généré en supprimant les marqueurs ```python
        code = re.sub(r"```(?:python)?\s*|```", "", code, flags=re.IGNORECASE)

        # Écrire le code principal
        f.write("\n\n# Main transformation function\n")
        f.write(code)
        f.write('\n\n\nif __name__ == "__main__":\n    main(spark)\n')


def Error_handler(ddl, error):
    model = ChatGoogleGenerativeAI(
        model="gemini-2.0-flash-lite",
        google_api_key=os.getenv("GEMINI_API_KEY2"),
        temperature=0,
        max_tokens=None,
        timeout=None,
        max_retries=2,
    )

    prompt = Prompt_Error(ddl, error)
    response = model.invoke(prompt)

    return response.content


def analyze_error(error_msg: str) -> str:
    try:
        llm = ChatGroq(
            temperature=0.3,
            groq_api_key=os.getenv("groq_api"),
            model_name=os.getenv("MODEL"),
        )

        messages = [
            SystemMessage(
                content=(
                    "You are a senior cloud database engineer specializing in Snowflake, assisting in a pipeline that migrates database schemas into Snowflake. "
                    "When given an error message, respond in this exact format:\n\n"
                    "## Error\n<Error message>\n\n"
                    "## Explain the Error\nYou have an error in <specific part> because <technical cause>.\n\n"
                    "## How to Fix it (Recommendation)\n<clear recommended fix>.\n\n"
                    "Focus on Snowflake syntax and behavior. Be precise, technical, and Explain with details"
                )
            ),
            HumanMessage(content=f"Error: {error_msg}"),
        ]
        response = llm.invoke(messages)
        return response.content
    except Exception as e:
        return f"Error analysis unavailable: {str(e)}"


# --- Fonction d'envoi d'email ---
def send_error_email(error_msg: str, analysis: str):
    """Sends an email with the error and its analysis."""
    try:
        msg = EmailMessage()
        msg["Subject"] = "🚨 Error in DDL workflow"
        msg["From"] = os.getenv("SMTP_USER")
        msg["To"] = os.getenv("EMAIL_TO")
        # Regex pattern
        pattern = r"## Error\n(.*?)\n\n## Explain the Error\n(.*?)\n\n## How to Fix it \(Recommendation\)\n(.*)"
        # Match and extract
        match = re.search(pattern, analysis, re.DOTALL)
        if match:
            error_part = match.group(1).strip()
            explain_part = match.group(2).strip()
            fix_part = match.group(3).strip()
        # Build HTML content
        email_content = f"""
        <html>
        <body>
            <p><strong>Dear Team,</strong></p>

            <p>An error occurred in the <strong>Snowflake Migration Pipeline</strong> process for <strong>HR DATABASE</strong>. Please find the details below:</p>

            <h3>🚫 Original Error</h3>
            <pre style="background-color:#f8f8f8;padding:10px;border-radius:5px;border:1px solid #ddd;">{error_msg}</pre>

            <h3>🔎 Technical Analysis</h3>
            <p>{explain_part}</p>

            <h3>✅ Recommended Fix</h3>
            <pre style="background-color:#f0fff0;padding:10px;border-radius:5px;border:1px solid #cce5cc;">{fix_part}</pre>

            <br>
            <p>Best regards,<br>
            <strong>Automation Migration System For Talan Tunisia</strong><br>
            IT Operations Team</p>
        </body>
        </html>
        """

        msg.set_content("This is a multipart message in HTML format.", subtype="plain")
        msg.add_alternative(email_content, subtype="html")

        with smtplib.SMTP(os.getenv("SMTP_HOST"), int(os.getenv("SMTP_PORT"))) as s:
            s.ehlo()
            s.starttls()
            s.ehlo()
            s.login(os.getenv("SMTP_USER"), os.getenv("SMTP_PASSWORD"))
            s.send_message(msg)

        logger.info("Email d'erreur envoyé avec succès")

    except Exception as e:
        logger.error(f"Échec envoi email: {str(e)}")


# def Snowflake_schema_info(data: Dict) -> Dict[str, Dict[str, Set[str]]]:
#     """
#     Extract table names, columns, primary keys, and foreign keys from JSON data.
#     Returns a dictionary with table names as keys and dictionaries with labeled sets
#     (c: columns, pk: primary_keys, fk: foreign_keys) as values.
#     """
#     result = defaultdict(lambda: {"c": set(), "pk": set(), "fk": set()})

#     for table_name, table_info in data.items():
#         columns = set()
#         primary_keys = set()
#         foreign_keys = set()

#         # Extract columns
#         for col in table_info.get("columns", []):
#             if isinstance(col, dict) and "name" in col:
#                 columns.add(col["name"])

#         # Extract primary keys
#         for pk in table_info.get("primary_keys", []):
#             primary_keys.add(pk)

#         # Extract foreign keys
#         for fk in table_info.get("foreign_keys", []):
#             if isinstance(fk, dict) and "column" in fk:
#                 foreign_keys.add(fk["column"])

#         result[table_name] = {"c": columns, "pk": primary_keys, "fk": foreign_keys}

#     return dict(result)


# def extract_oracle_schema(data: Dict) -> Dict[str, Dict[str, Set[str]]]:
#     """
#     Extract table names, columns, primary keys, and foreign keys from Oracle schema JSON data.
#     Returns a dictionary with table names as keys and dictionaries with labeled sets
#     (c: columns, pk: primary_keys, fk: foreign_keys) as values.
#     """
#     result = defaultdict(lambda: {"c": set(), "pk": set(), "fk": set()})

#     # Access the 'tables' key in the JSON
#     tables = data.get("tables", {})

#     for table_name, table_info in tables.items():
#         columns = set()
#         primary_keys = set()
#         foreign_keys = set()

#         # Extract columns
#         for col in table_info.get("columns", []):
#             if isinstance(col, dict) and "name" in col:
#                 columns.add(col["name"])

#         # Extract primary keys
#         pk_info = table_info.get("primary_key", {})
#         if isinstance(pk_info, dict):
#             for pk in pk_info.get("columns", []):
#                 primary_keys.add(pk)

#         # Extract foreign keys
#         for fk in table_info.get("foreign_keys", []):
#             if isinstance(fk, dict):
#                 for fk_col in fk.get("columns", []):
#                     foreign_keys.add(fk_col)

#         result[table_name] = {"c": columns, "pk": primary_keys, "fk": foreign_keys}

#     return dict(result)


def Snowflake_schema_info(snowflake_schema: Dict) -> Dict[str, Dict[str, Set[str]]]:
    """
    Extract table names, columns, primary keys, and foreign keys from Snowflake schema dictionary.
    Returns a dictionary with table names as keys and dictionaries with labeled sets
    (c: columns, pk: primary_keys, fk: foreign_keys) as values.
    """
    result = defaultdict(lambda: {"c": set(), "pk": set(), "fk": set()})

    for table_name, table_info in snowflake_schema.items():
        columns = set()
        primary_keys = set()
        foreign_keys = set()

        # Extract columns
        for col in table_info.get("columns", []):
            if isinstance(col, dict) and "name" in col:
                columns.add(col["name"])

        # Extract primary keys
        for pk in table_info.get("primary_keys", []):
            primary_keys.add(pk)

        # Extract foreign keys
        for fk in table_info.get("foreign_keys", []):
            if isinstance(fk, dict) and "column" in fk:
                foreign_keys.add(fk["column"])

        result[table_name] = {"c": columns, "pk": primary_keys, "fk": foreign_keys}
    print(dict(result))
    return dict(result)


def extract_oracle_schema(oracle_schema: Dict) -> Dict[str, Dict[str, Set[str]]]:
    """
    Extract table names, columns, primary keys, and foreign keys from Oracle schema dictionary.
    Returns a dictionary with table names as keys and dictionaries with labeled sets
    (c: columns, pk: primary_keys, fk: foreign_keys) as values.
    """
    result = defaultdict(lambda: {"c": set(), "pk": set(), "fk": set()})

    tables = oracle_schema.get("tables", {})

    for table_name, table_info in tables.items():
        columns = set()
        primary_keys = set()
        foreign_keys = set()

        # Extract columns
        for col in table_info.get("columns", []):
            if isinstance(col, dict) and "name" in col:
                columns.add(col["name"])

        # Extract primary keys
        pk_info = table_info.get("primary_key", {})
        if isinstance(pk_info, dict):
            for pk in pk_info.get("columns", []):
                primary_keys.add(pk)

        # Extract foreign keys
        for fk in table_info.get("foreign_keys", []):
            if isinstance(fk, dict):
                for fk_col in fk.get("columns", []):
                    foreign_keys.add(fk_col)

        result[table_name] = {"c": columns, "pk": primary_keys, "fk": foreign_keys}
    print(dict(result))
    return dict(result)
