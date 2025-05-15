from typing import Optional, TypedDict, Dict, Literal
from langgraph.graph import StateGraph, END
from utils.SnowflakeSchemaExtractor import SnowflakeSchemaExtractor
import json
from utils.utils_functions import *
import re
import subprocess
import os

###############################################################


## State Graph
class State(TypedDict):
    oracle_schema: Optional[str]
    snowflake_schema: Optional[str]
    comparaison_result: Optional[str]
    mapping: Optional[str]
    Dict_mapping: Optional[Dict]
    current_table: Optional[str]
    spark_code: Optional[Dict[str, str]]
    status: Optional[str]
    error: Optional[str]


## Node: validation snowflake schema with ODS schema
def Schema_validation(state: State):

    # Récupérer le schéma Snowflake
    extractor = SnowflakeSchemaExtractor()
    snowflake_schema = extractor.get_schema()
    oracle_schema = state["oracle_schema"]
    # oracle_schema_json = json.dumps(oracle_schema, indent=2)
    # snowflake_schema_json = json.dumps(snowflake_schema, indent=2)
    result = schema_comparisation(oracle_schema, snowflake_schema)
    # print(result)
    return {"comparaison_result": result, "snowflake_schema": snowflake_schema}


## Node: Mapping Generation
def Generate_Mapping(state: State):
    with open("HR.json", "r", encoding="utf-8") as f:
        HR_Schema = json.load(f)
    response = mapping_generation(HR_Schema, state["snowflake_schema"])
    Dictionary = formulated_mapping(response)
    # print(response)
    return {"mapping": response, "Dict_mapping": Dictionary}


# Node: Selection of table to be processed
def select_table(state: State):
    mapping = state["mapping"]
    processed_tables = set(state["spark_code"].keys() if state["spark_code"] else [])
    # print(processed_tables)
    # Trouver la première table non traitée (ordre de dépendance important)
    for table_name in get_processing_order(mapping):
        if table_name not in processed_tables:
            # print("processed_table:", table_name)
            return {"current_table": table_name}

    # Toutes les tables sont traitées
    return {"current_table": "__COMPLETED__"}


# Node: Generate spark code for selected table
def generate_spark_code(state: State):
    table_name = state["current_table"]
    table_mapping = state["Dict_mapping"][table_name]
    spark_code = generate_table_code(table_name, table_mapping)
    # print("Sparkcodepart", spark_code)

    # Récupérer le dictionnaire existant ou en créer un nouveau si inexistant
    existing_spark_code = state.get("spark_code", {})

    return {
        "spark_code": {
            **existing_spark_code,
            table_name: spark_code,
        },  # Fusionner les dictionnaires
        "validation_errors": None,
    }


## Node : Write Spark Code Result
def WriteSparkCode(state: State):
    spark_code_dict = state.get("spark_code", {})

    # Nom du fichier de sortie²
    output_file = "spark-scripts/spark_transformations.py"

    # Écrire tous les codes dans un seul fichier
    with open(output_file, "a", encoding="utf-8") as f:
        # En-tête du fichier
        f.write("# Transformations Spark générées automatiquement\n")
        f.write("# Contient les transformations pour toutes les tables\n\n")

        # Écrire le code pour chaque table
        for table_name, code in spark_code_dict.items():
            code = re.sub(r"```(?:python)?\s*|```", "", code, flags=re.IGNORECASE)
            f.write(f"\n{'#' * 80}\n")
            f.write(f"# Transformation pour la table {table_name}\n")
            f.write(f"{'#' * 80}\n\n")
            f.write(code)
            f.write("\n\n")

    # print(f"Tous les codes Spark ont été écrits dans {output_file}")
    return {"status": f"Fichier {output_file} généré avec succès"}


## Node: Main Program Preparation
def ExecETL(state: State):
    # 1. Préparation du programme Spark
    source_table, target_table = get_tables(state["mapping"])

    # 2. Génération du code Spark
    with open("spark-scripts/spark_transformations.py", "r", encoding="utf-8") as f:
        function = f.read()
    main = generate_main(function, source_table, target_table)
    write_main(main)

    bat_path = os.path.abspath("run.bat")

    try:
        result = subprocess.run(
            [bat_path],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        print("✅ Script exécuté avec succès :")
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print("❌ Erreur lors de l'exécution du script :")
        print("Code retour :", e.returncode)
        print("stdout :", e.stdout or "[stdout vide]")
        print("stderr :", e.stderr or "[stderr vide]")


## Routing
def route_by_status(state: State) -> Literal["continue", "end"]:
    """Routing logic based on validation result"""
    if state.get("comparaison_result") == False:
        return "end"
    else:
        return "continue"


def route_spark(
    state: State,
) -> Literal["Next", "Rollback"]:  # J'ai corrigé le type de retour
    """Routing logic based on table execution step"""
    if state.get("current_table") == "__COMPLETED__":
        return "Next"
    else:
        return "Rollback"


# Create the workflow
builder = StateGraph(State)

# Add nodes
builder.add_node("Schema_Validation", Schema_validation)
builder.add_node("Mapping_Creation", Generate_Mapping)
builder.add_node("Table_Selection", select_table)
builder.add_node("SparkJob_Creation", generate_spark_code)
builder.add_node("JobCode_Storage", WriteSparkCode)
builder.add_node("ExecETL", ExecETL)
# Set up edges
builder.set_entry_point("Schema_Validation")
# builder.add_edge("Schema_Validation", "Mapping_Creation")
builder.add_edge("Mapping_Creation", "Table_Selection")
builder.add_edge("SparkJob_Creation", "Table_Selection")
# builder.add_edge("select_table", "SparkJob")
# builder.add_edge("select_table", "WriteCode")
builder.add_edge("JobCode_Storage", "ExecETL")
builder.add_edge("ExecETL", END)


## Conditional_edges Setup
builder.add_conditional_edges(
    "Schema_Validation",
    route_by_status,
    {
        "continue": "Mapping_Creation",  # If validation passes
        "end": END,  # If validation fails
    },
)
builder.add_conditional_edges(
    "Table_Selection",
    route_spark,
    {
        "Next": "JobCode_Storage",  # If all table are treated
        "Rollback": "SparkJob_Creation",  # Else, treate next table
    },
)


# Compile the graph

DW_VALIDATE_AND_LOAD = builder.compile()
