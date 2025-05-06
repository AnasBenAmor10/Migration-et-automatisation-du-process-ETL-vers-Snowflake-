import logging
import os
from dotenv import load_dotenv
import re
from langchain_google_genai import ChatGoogleGenerativeAI
from Prompt import *
import subprocess
import sys
from typing import Tuple

# Chargement des variables d'environnement
load_dotenv()

# Configuration du logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



## --- Fonction de comparaison
def schema_comparisation(oracle_schema_json: str, snowflake_schema_json: str) -> str:
    prompt = f"""
        STRICTLY respond with ONLY 'true' or 'false' based on this schema comparison:

        Return 'true' ONLY IF ALL these conditions are met:
        
        1. ALL Oracle tables exist in Snowflake (same table, case-insensitive)
        2. Each table contains ALL original columns (same column names, case-insensitive)
        3. ALL primary keys are preserved (same columns designated as PK)
        4. ALL foreign key relationships exist (same source/target columns)

        Return 'false' if ANY of these occur:
        - Any Oracle table is missing in Snowflake
        - Any column is missing from original tables
        - Any primary key is missing or different
        - Any foreign key relationship is missing

        EXPLICITLY IGNORE:
        - Data type differences (VARCHAR2/STRING, NUMBER/INTEGER, etc.)
        - Column order differences
        - Additional Snowflake columns/tables not in Oracle
        - Comments, metadata, or descriptions
        - Case sensitivity in names
        - Storage-specific attributes

        Type equivalencies to accept:
        - VARCHAR2 ↔ STRING ↔ TEXT
        - NUMBER ↔ INTEGER ↔ FLOAT ↔ DECIMAL
        - DATE ↔ TIMESTAMP
        - CHAR ↔ STRING
        - Any other reasonable type variations

        Oracle Schema (Reference):
        {oracle_schema_json}

        Snowflake Schema (To Compare):
        {snowflake_schema_json}

        Your response must be EXACTLY 'true' or 'false' with:
        - NO additional text
        - NO explanations
        - NO punctuation
        - NO JSON formatting
        - ONLY lowercase true/false
        """

    llm = ChatGoogleGenerativeAI(
        model="gemini-2.0-flash-lite",
        google_api_key=os.getenv("GEMINI_API_KEY"),
        temperature=0,
        max_tokens=None,
        timeout=None,
        max_retries=2,
    )

    response = llm.invoke(prompt)
    return response.content


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
    output_file = "spark_transformations.py"


    # Écrire tous les codes dans un seul fichier
    with open(output_file, "a", encoding="utf-8") as f:
        # Écrire l'en-tête
        # f.write(header)

        # Nettoyer le code généré en supprimant les marqueurs ```python
        code = re.sub(r"```(?:python)?\s*|```", "", code, flags=re.IGNORECASE)

        # Écrire le code principal
        f.write("\n\n# Main transformation function\n")
        f.write(code)
        f.write('\n\n\nif __name__ == "__main__":\n    main(spark)\n')


def ExecCode() -> Tuple[bool, str]:
    """
    Version améliorée avec vérifications supplémentaires
    """
    script_path = "./run_spark_script.sh"

    try:
        # Vérification que le script est exécutable
        subprocess.run(["chmod", "+x", script_path], check=True)

        # Exécution avec timeout de sécurité (30 min)
        result = subprocess.run(
            [script_path],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=1800,  # 30 minutes timeout
        )

        output = result.stdout.strip()
        return (True, output)

    except subprocess.TimeoutExpired:
        return (False, "Timeout: Le script a pris trop de temps à s'exécuter")

    except subprocess.CalledProcessError as e:
        error_msg = "Erreur d'exécution:\n"
        error_msg += f"Code: {e.returncode}\n"
        error_msg += f"Sortie:\n{e.stdout}\n"
        error_msg += f"Erreurs:\n{e.stderr}"
        return (False, error_msg)

    except Exception as e:
        return (False, f"Erreur inattendue: {str(e)}")
