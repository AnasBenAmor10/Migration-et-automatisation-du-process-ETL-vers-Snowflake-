from typing import Optional, TypedDict, Dict, Literal
from langgraph.graph import StateGraph, END
from OracleSchemaExporter import OracleSchemaExporter
from SnowflakeSchemaGenerator import SnowflakeSchemaGenerator
from DDLCreator import DDLCreator
from dotenv import load_dotenv
import json
import logging
from OracleDDLExecutor import OracleDDLExecutor

logger = logging.getLogger(__name__)
load_dotenv()
###############################################################


## State Graph
class State(TypedDict):
    Source_schema: Optional[Dict]
    New_Schema: Optional[Dict]
    ddl: Optional[str]
    status: Optional[str]
    error: Optional[str]


## Node: Export schema source from oracle database
def Export_Source(state: State):
    exporter = OracleSchemaExporter()
    try:
        ## Connect to Oracle Database
        exporter.connect()
        json_file = exporter.export_schema_to_json("HR")
        with open(json_file, "r") as f:
            schema = json.load(f)
    except Exception as e:
        error_msg = f"❌ Failed to export source schema: {str(e)}"
        logger.error(error_msg)
        state["error"] = error_msg
    finally:
        exporter.close_connection()
    state["Source_schema"] = schema
    state["status"] = "Source schema exported sucessfully !"
    return state


## Node: Generate new model for a old schema
def Generate_New_Model(state: State):
    generator = SnowflakeSchemaGenerator()
    result = generator.generate_model(state["Source_schema"])
    state["New_Schema"] = result
    return state


## Node : Create DDL for provided schema
def CreateDDL(state: State):
    try:
        logger.info("🛠️ Generating DDL for the provided schema...")
        creator = DDLCreator(state["New_Schema"])
        result = creator.generate_ddl()

        state["ddl"] = result["ddl"]
        state["status"] = "✅ DDL script generated successfully."
        logger.info("✅ DDL generation complete.")

    except Exception as e:
        error_msg = f"❌ Failed to generate DDL: {str(e)}"
        logger.error(error_msg)
        state["error"] = error_msg

    return state


## Node : Execute the DDL into oracle database
def Exec(state: State):
    executor = OracleDDLExecutor()
    ddl_script = state["ddl"]

    if not ddl_script:
        error_msg = "❌ No DDL script found in state. Cannot execute."
        logger.error(error_msg)
        state["error"] = error_msg
        return state

    try:
        logger.info("🔌 Connecting to Oracle database for DDL execution...")
        executor.connect()

        logger.info("📂 Creating (or recreating) schema...")
        executor.create_schema(recreate=True)

        logger.info("🚀 Executing DDL script...")
        executor.execute_ddl(ddl_script)

        state["status"] = "✅ DDL script executed successfully."
        logger.info("✅ DDL execution complete.")

    except Exception as e:
        error_msg = f"❌ DDL execution failed: {str(e)}"
        logger.error(error_msg)
        state["error"] = error_msg

    finally:
        executor.close()
        logger.info("🔒 Oracle connection closed.")

    return state


# Create the workflow
builder = StateGraph(State)

# Add nodes
builder.add_node("Export Schema", Export_Source)
builder.add_node("Modelisation", Generate_New_Model)
builder.add_node("Create DDL", CreateDDL)
builder.add_node("Execute The DDL", Exec)

# Set up edges
builder.set_entry_point("Export Schema")
builder.add_edge("Export Schema", "Modelisation")
builder.add_edge("Modelisation", "Create DDL")
builder.add_edge("Create DDL", "Execute The DDL")
builder.add_edge("Execute The DDL", END)


# Compile the graph

Model = builder.compile()
initial_state = State(
    Source_schema=None,
    New_Schema=None,
    error=None,
    status=None,
    ddl=None,
)
result = Model.invoke(initial_state)
