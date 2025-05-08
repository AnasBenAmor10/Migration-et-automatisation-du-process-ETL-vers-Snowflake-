from typing import Optional, TypedDict, Dict
from langgraph.graph import StateGraph, END
from dotenv import load_dotenv
import logging
from datetime import datetime
from utils.SchemaExporter import SchemaExporter
from utils.DDL import SnowflakeDDLCreator
from utils.SnowflakeExecution import SnowflakeDDLExecutor
from utils.utils_functions import *

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
    success: Optional[bool]
    analysis: Optional[str]
    error: Optional[str]
    retry_count: int
    max_retries: int


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
    try:
        executor.execute(state["ddl"])
        state["success"] = True
        state["error"] = None
    except Exception as e:
        state["success"] = False
        state["error"] = str(e)
    return state


## Node: Reflexion Agent Node
def ReflectNode(state: State) -> Dict:
    """Reflection agent that analyzes EXECDDL errors and corrects the DDL."""
    if state["retry_count"] >= state["max_retries"]:
        return {"error": "Max retries reached. Could not resolve DDL errors."}
    corrected_ddl = Error_handler(state["ddl"], state["error"])
    return {
        "ddl": corrected_ddl,
        "error": None,
        "retry_count": state["retry_count"] + 1,
    }


# --- Nœud erreur ---
# def error_handler_node(state: State) -> Dict:
#     if state["error"] is not None and len(state["error"]) > 0:
#         state["analysis"] = analyze_error(state["error"])
#         send_error_email(state["error"], state["analysis"])
#     return state


def trigger_validation_graph(state: State):
    logger.info("Déclenchement du graphe de validation")

    from ETL import (
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


def route_after_exec(state: State) -> str:
    """Determine the next node after EXECDDL based on execution results.

    Returns:
        str: Next node name based on these conditions:
            - "trigger_validation_graph" if execution succeeded
            - "ReflectNode" if execution failed and retries remain
            - "END" if max retries exceeded
    """
    if state.get("success", False):
        logger.info("DDL executed successfully, proceeding to validation")
        return "trigger_validation_graph"
    elif state["retry_count"] < state["max_retries"]:
        logger.warning(
            f"DDL execution failed (attempt {state['retry_count']+1}/{state['max_retries']}), reflecting..."
        )
        return "ReflectNode"
    else:
        logger.error(f"Max retries ({state['max_retries']}) reached with errors")
        return "END"


# Create the workflow
builder = StateGraph(State)

# Add nodes
builder.add_node("extract_schema", extract_schema_node)
builder.add_node("DDLCreator", DDLNode)
builder.add_node("ExecDDL", EXECDDL)
builder.add_node("ReflectNode", ReflectNode)
# builder.add_node("handle_error", error_handler_node)
builder.add_node("trigger_validation_graph", trigger_validation_graph)

# Set up edges
builder.set_entry_point("extract_schema")
builder.add_edge("extract_schema", "DDLCreator")
builder.add_edge("DDLCreator", "ExecDDL")
builder.add_edge("ExecDDL", "trigger_validation_graph")
builder.add_edge("trigger_validation_graph", END)
builder.add_edge("ReflectNode", "ExecDDL")  # Go back to recreate DDL after reflection

# builder.add_conditional_edges(
#     "ExecDDL",
#     lambda state: "error" if "error" in state else "END",
#     {"error": "handle_error"},  # Corrected to use handle_error
# )
builder.add_conditional_edges(
    "ExecDDL",
    route_after_exec,
    {
        "ReflectNode": "ReflectNode",
        "trigger_validation_graph": "trigger_validation_graph",
        "END": END,
    },
)

import json

# Compile the graph
logger.info("Compiling LangGraph workflow")
DW_CREATE = builder.compile()
# Execute the graph
logger.info("Executing LangGraph workflow")
initial_state = State(
    schema=None,
    ddl=None,
    error=None,
    success=False,
    analysis=None,
    max_retries=5,
    retry_count=0,
)
result = DW_CREATE.invoke(initial_state)
