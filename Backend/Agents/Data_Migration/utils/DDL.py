from langchain_core.prompts import PromptTemplate
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain.agents import AgentExecutor, Tool
from langchain.agents.format_scratchpad import format_log_to_str
from langchain.agents.output_parsers import ReActSingleInputOutputParser
from langchain.tools.render import render_text_description
import os
from dotenv import load_dotenv
from utils.Prompt import *

load_dotenv()


class SnowflakeDDLCreator:
    def __init__(self, formatted_json):
        """Initialise avec un schéma JSON formaté pour générer du DDL Snowflake."""
        self.formatted_json = formatted_json
        self.schema = formatted_json
        self.ddl_script = None

        self.llm = ChatGoogleGenerativeAI(
            model="gemini-2.0-flash-lite",
            google_api_key=os.getenv("GEMINI_API_KEY"),
            temperature=0,
            max_tokens=None,
            timeout=None,
            max_retries=2,
        )

        self.tools = [
            Tool(
                name="GenerateSnowflakeDDL",
                func=self.generate_base_ddl,
                description="Génère le DDL Snowflake de base à partir du schéma.",
            ),
            Tool(
                name="ValidateSnowflakeSyntax",
                func=self.validate_syntax,
                description="Valide la syntaxe SQL spécifique à Snowflake.",
            ),
            Tool(
                name="AddConstraintsToDDL",
                func=self.add_constraints,
                description="Ajoute des contraintes (PRIMARY KEY, UNIQUE, etc.) dans Snowflake.",
            ),
            Tool(
                name="SuggestClusteringKeys",
                func=self.add_clustering,
                description="Ajoute des CLUSTER BY recommandés pour Snowflake selon les bonnes pratiques.",
            ),
            Tool(
                name="DocumentDDL",
                func=self.add_comments,
                description="Ajoute des COMMENT sur les tables et colonnes pour documentation.",
            ),
        ]

        self.agent = self._create_agent()

    def _create_agent(self):
        """Crée l'agent de génération DDL avec outils Snowflake."""

        prompt = PromptTemplate.from_template(
            get_main_agent_prompt()
            + "\n\n"
            + "## TOOLS:\n{tools}\n\n"
            + "## SCHEMA DEFINITION:\n{schema}\n\n"
            + "Thought:{agent_scratchpad}"
        )

        llm_with_stop = self.llm.bind(stop=["\nObservation:"])

        agent = (
            {
                "schema": lambda x: x["schema"],
                "agent_scratchpad": lambda x: format_log_to_str(
                    x["intermediate_steps"]
                ),
                "tools": lambda x: render_text_description(x["tools"]),
                "tool_names": lambda x: ", ".join([t.name for t in x["tools"]]),
            }
            | prompt
            | llm_with_stop
            | ReActSingleInputOutputParser()
        )

        return AgentExecutor(
            agent=agent,
            tools=self.tools,
            verbose=True,
            handle_parsing_errors=True,
            max_iterations=10,
        )

    def generate_base_ddl(self, input):
        """Génère le DDL de base en SQL Snowflake."""
        chain = (
            PromptTemplate.from_template(
                get_base_ddl_prompt()
                + "\n\nGénère uniquement le DDL Snowflake à partir du schéma suivant :\n{schema}"
            )
            | self.llm
        )

        ddl = chain.invoke({"schema": self.formatted_json}).content
        self.ddl_script = ddl
        return ddl

    def validate_syntax(self, ddl):
        """Valide la syntaxe SQL pour Snowflake."""
        chain = (
            PromptTemplate.from_template(
                get_validation_prompt() + "\n\nValide ce DDL Snowflake :\n{ddl}"
            )
            | self.llm
        )
        return chain.invoke({"ddl": ddl}).content

    def add_constraints(self, ddl):
        """Ajoute les contraintes PRIMARY KEY, FOREIGN KEY, etc."""
        chain = (
            PromptTemplate.from_template(
                get_constraints_prompt()
                + "\n\nAjoute les contraintes nécessaires dans ce DDL Snowflake :\n{ddl}"
            )
            | self.llm
        )

        updated = chain.invoke({"schema": self.formatted_json, "ddl": ddl}).content
        self.ddl_script = updated
        return updated

    def add_clustering(self, ddl):
        """Ajoute des CLUSTER BY en fonction du schéma et des bonnes pratiques."""
        chain = (
            PromptTemplate.from_template(
                get_clustering_prompt()
                + "\n\nAjoute les CLUSTER BY si nécessaire dans ce DDL Snowflake :\n{ddl}"
            )
            | self.llm
        )

        clustered = chain.invoke({"schema": self.formatted_json, "ddl": ddl}).content
        self.ddl_script = clustered
        return clustered

    def add_comments(self, ddl):
        """Ajoute des commentaires de documentation sur les tables et colonnes."""
        chain = (
            PromptTemplate.from_template(
                get_comments_prompt()
                + "\n\nAjoute les COMMENT dans ce DDL Snowflake :\n{ddl}"
            )
            | self.llm
        )

        documented = chain.invoke({"schema": self.formatted_json, "ddl": ddl}).content
        self.ddl_script = documented
        return documented

    def generate_ddl(self):
        """Orchestre le processus complet de génération de DDL."""
        try:
            result = self.agent.invoke(
                {"schema": self.formatted_json, "tools": self.tools}
            )

            if isinstance(result, dict) and "output" in result:
                output = result["output"]
                if output.startswith("FINAL_DDL:"):
                    self.ddl_script = output.replace("FINAL_DDL:", "").strip()

            if self.ddl_script:
                self.ddl_script = "\n".join(
                    [s for s in self.ddl_script.split("\n") if s.strip()]
                )

            return {
                "ddl": self.ddl_script,
                "validation": (
                    self.validate_syntax(self.ddl_script)
                    if self.ddl_script
                    else "No DDL generated"
                ),
                "status": "completed",
            }
        except Exception as e:
            return {
                "ddl": self.ddl_script,
                "validation": f"Erreur lors de la génération : {str(e)}",
                "status": "failed",
            }
