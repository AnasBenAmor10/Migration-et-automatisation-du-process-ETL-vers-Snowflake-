from typing import Optional, TypedDict, Dict
from langgraph.graph import StateGraph, END
from Agents.Data_Modelation.src.OracleSchemaExporter import OracleSchemaExporter
from Agents.Data_Modelation.src.SnowflakeSchemaGenerator import SnowflakeSchemaGenerator
from Agents.Data_Modelation.src.DDLCreator import DDLCreator
from Agents.Data_Modelation.src.OracleDDLExecutor import OracleDDLExecutor
from Agents.Data_Modelation.src.utils.reflexion import ModelReflectionAgent
from dotenv import load_dotenv
import json
import logging
logger = logging.getLogger(__name__)
load_dotenv()

class State(TypedDict):
    Source_schema: Optional[Dict]
    New_Schema: Optional[Dict]
    ddl: Optional[str]
    status: Optional[str]
    error: Optional[str]
    schema_name: Optional[str]
    current_node: Optional[str]
    model_iterations: Optional[List[Dict]]  
    critiques: Optional[List[str]]  
    current_iteration: Optional[int]  
    max_iterations: Optional[int]

def Export_Source(state: State):
    exporter = OracleSchemaExporter()
    try:
        exporter.connect()
        schema_name = state["schema_name"]
        json_file = exporter.export_schema_to_json(schema_name)
        with open(json_file, "r") as f:
            schema = json.load(f)
    except Exception as e:
        error_msg = f"❌ Failed to export source schema: {str(e)}"
        logger.error(error_msg)
        state["error"] = error_msg
    finally:
        exporter.close_connection()
    state["Source_schema"] = schema
    state["status"] = "Source schema exported successfully!"
    return state

def Generate_New_Model(state: State):
    # generator = SnowflakeSchemaGenerator()
    # result = generator.generate_model(state["Source_schema"])
    # state["New_Schema"] = result
    # state["status"] = "New model generated successfully!"
    # return state
    # Initialize reflection agent and state tracking
    if "model_iterations" not in state:
        state["model_iterations"] = []
        state["critiques"] = []
        state["current_iteration"] = 0
        state["max_iterations"] = 3 
        
    reflection_agent = ModelReflectionAgent()
    source_schema = state["Source_schema"]
    
    # Generate initial model
    if state["current_iteration"] == 0:
        current_model = reflection_agent.generate_initial_model(source_schema)
        state["model_iterations"].append(current_model)
        state["status"] = f"Initial model generated (Iteration {state['current_iteration']+1})"
        state["current_iteration"] += 1
        return state
    
    # Get the last model version
    current_model = state["model_iterations"][-1]
    
    # Generate critique
    critique = reflection_agent.critique_model(source_schema, current_model)
    state["critiques"].append(critique)
    
    # Check if we've reached satisfactory quality or max iterations
    if state["current_iteration"] >= state["max_iterations"]:
        state["New_Schema"] = current_model
        state["status"] = f"Final model selected after {state['current_iteration']} iterations"
        return state
    
    # Refine the model
    refined_model = reflection_agent.refine_model(current_model, critique)
    state["model_iterations"].append(refined_model)
    state["current_iteration"] += 1
    state["status"] = f"Model refined (Iteration {state['current_iteration']})"
    
    # Continue to next iteration
    return state

def CreateDDL(state: State):
    try:
        creator = DDLCreator(state["New_Schema"])
        result = creator.generate_ddl()
        state["ddl"] = result["ddl"]
        state["status"] = "DDL script generated successfully."
    except Exception as e:
        error_msg = f"❌ Failed to generate DDL: {str(e)}"
        logger.error(error_msg)
        state["error"] = error_msg
    return state

def Exec(state: State):
    executor = OracleDDLExecutor()
    ddl_script = state["ddl"]
    if not ddl_script:
        error_msg = "❌ No DDL script found in state."
        logger.error(error_msg)
        state["error"] = error_msg
        return state
    try:
        executor.connect()
        executor.create_schema(recreate=True)
        executor.execute_ddl(ddl_script)
        state["status"] = "DDL script executed successfully."
    except Exception as e:
        error_msg = f"❌ DDL execution failed: {str(e)}"
        logger.error(error_msg)
        state["error"] = error_msg
    finally:
        executor.close()
    return state

##Graph Model Class
class GraphModel:
    def __init__(self):
        self.builder = StateGraph(State)
        self._setup_nodes_and_edges()
        self.Model = self.builder.compile()
        self.execution_path = []
        self.ui_callback = lambda msg: None 


    def _setup_nodes_and_edges(self):
        # Ajout des nœuds
        self.builder.add_node("Export Schema", self._wrap_node(Export_Source, "Export Schema"))
        self.builder.add_node("Modelisation", self._wrap_node(Generate_New_Model, "Modelisation"))
        self.builder.add_node("Create DDL", self._wrap_node(CreateDDL, "Create DDL"))
        self.builder.add_node("Execute The DDL", self._wrap_node(Exec, "Execute The DDL"))

        # Configuration des edges
        self.builder.set_entry_point("Export Schema")
        self.builder.add_edge("Export Schema", "Modelisation")
        self.builder.add_edge("Modelisation", "Create DDL")
        self.builder.add_edge("Create DDL", "Execute The DDL")
        self.builder.add_edge("Execute The DDL", END)

    def set_ui_callback(self, callback):
        """Définit le callback pour envoyer des messages à l'UI"""
        self.ui_callback = callback

    def _wrap_node(self, node_func, node_name):
        def wrapped_node(state: State):
            # Message de début structuré
            node_info = {
                "current_node": node_name,
                "progress": self._calculate_progress(node_name), 
                "message": f"Début du nœud: {node_name}",
                "__type__": "node_update"
            }
            
            try:
                if self.ui_callback:
                    self.ui_callback(node_info)
                
                new_state = node_func(state)
                
                # Message de succès
                if self.ui_callback:
                    self.ui_callback({
                        "current_node": node_name,
                        "progress": self._calculate_progress(node_name, True),
                        "message": f"✅ {node_name} terminé",
                        "__type__": "node_success"
                    })
                
                return new_state
                
            except Exception as e:
                error_msg = str(e)
                if self.ui_callback:
                    self.ui_callback({
                        "current_node": node_name,
                        "progress": 0,
                        "message": f"❌ Erreur dans {node_name}: {error_msg}",
                        "__type__": "node_error"
                    })
                state["error"] = error_msg
                return state
        
        return wrapped_node

    def _calculate_progress(self, node_name, completed=False):
        """Calcule la progression basée sur le nœud actuel"""
        nodes_order = ["Export Schema", "Modelisation", "Create DDL", "Execute The DDL"]
        try:
            index = nodes_order.index(node_name)
            base_progress = index / len(nodes_order)
            return base_progress + (0.2 if completed else 0)
        except ValueError:
            return 0

    def invoke(self, initial_state: State):
        self.execution_path = []
        return self.Model.invoke(initial_state)

    def get_execution_path(self):
        return " → ".join(self.execution_path)

# Créez l'instance globale à la fin
Model = GraphModel()