from typing import Optional, TypedDict, Dict
from langgraph.graph import StateGraph, END
from dotenv import load_dotenv
import logging
from datetime import datetime
from utils.SchemaExporter import SchemaExporter
from utils.DDL import SnowflakeDDLCreator
from utils.SnowflakeExecution import SnowflakeDDLExecutor
from Snowflake_Schema_Creation.utils.utils_function import *

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# Load environment variables
load_dotenv()


class State(TypedDict):
    schema: Optional[dict]
    ddl: Optional[str]
    error: Optional[str]
    sucess: Optional[str]
    analysis: Optional[str]


def extract_schema_node(state: State):
    logger.info("Starting schema extraction node")
    try:
        # Call the schema extraction function
        exec = SchemaExporter()
        exec.connect()
        schema = exec.export_schema_to_json("ODS_HR")
        print("Extracted Schema:", json.dumps(schema, indent=2))
        return {"schema": schema}
    except Exception as e:
        return {"error": str(e)}


def DDLNode(state: State):
    creator = SnowflakeDDLCreator(state["schema"])
    result = creator.generate_ddl()
    return {"ddl": result["ddl"]}


def EXECDDL(state: State):
    """Wrapper function for LangGraph compatibility."""
    executor = SnowflakeDDLExecutor()
    return executor.execute(state["ddl"])


# --- Nœud erreur ---
def error_handler_node(state: State) -> Dict:
    if state["error"] is not None and len(state["error"]) > 0:
        state["analysis"] = analyze_error(state["error"])
        send_error_email(state["error"], state["analysis"])
    return state


def trigger_validation_graph(state: State):
    logger.info("Déclenchement du graphe de validation")

    from Data_Migration import (
        DW_VALIDATE_AND_LOAD,
    )

    validation_input = State(
        oracle_schema=state["schema"],
        snowflake_schema=None,
        comparaison_result=None,
        mapping=None,
        current_table=None,
        spark_code={},
        status=None,
    )

    DW_VALIDATE_AND_LOAD.invoke(validation_input)


# Create the workflow
builder = StateGraph(State)

# Add nodes
builder.add_node("extract_schema", extract_schema_node)
builder.add_node("DDLCreator", DDLNode)
builder.add_node("ExecDDL", EXECDDL)
builder.add_node("handle_error", error_handler_node)
builder.add_node("trigger_validation_graph", trigger_validation_graph)

# Set up edges
builder.set_entry_point("extract_schema")
builder.add_edge("extract_schema", "DDLCreator")
builder.add_edge("DDLCreator", "ExecDDL")
builder.add_edge("ExecDDL", "trigger_validation_graph")
builder.add_edge("trigger_validation_graph", END)


builder.add_conditional_edges(
    "ExecDDL",
    lambda state: "error" if "error" in state else "END",
    {"error": "handle_error"},  # Corrected to use handle_error
)


import json

# Compile the graph
logger.info("Compiling LangGraph workflow")
DW_CREATE = builder.compile()
# Execute the graph
logger.info("Executing LangGraph workflow")
initial_state = State(schema=None, ddl=None, error=None, success=None, analysis=None)
result = DW_CREATE.invoke(initial_state)
