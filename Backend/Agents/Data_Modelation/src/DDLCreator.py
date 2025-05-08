from langchain_core.prompts import PromptTemplate
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain.agents import AgentExecutor, Tool
from langchain.agents.format_scratchpad import format_log_to_str
from langchain.agents.output_parsers import ReActSingleInputOutputParser
from langchain.tools.render import render_text_description
import json
import sqlparse
from utils.Config import GEMINI_API_KEY
from utils.Prompt import *

class DDLCreator:
    def __init__(self, formatted_json):
        """Initialize with pre-formatted JSON string"""
        self.formatted_json = formatted_json
        self.schema = json.loads(formatted_json)
        self.llm = ChatGoogleGenerativeAI(
            model="gemini-2.0-flash-lite",
            google_api_key=GEMINI_API_KEY,
            temperature=0,
            max_tokens=None,
            timeout=None,
            max_retries=2,
        )
        self.ddl_script = None

        # Set up tools
        self.tools = [
            Tool(
                name="GenerateBaseDDL",
                func=self.generate_base_ddl,
                description="Generates initial DDL based on schema structure",
            ),
            Tool(
                name="ValidateSyntax",
                func=self.validate_syntax,
                description="Validates SQL syntax of generated DDL",
            ),
            Tool(
                name="OptimizeForOracle",
                func=self.optimize_for_oracle,
                description="Adjusts DDL for Oracle-specific requirements",
            ),
            Tool(
                name="AddConstraints",
                func=self.add_constraints,
                description="Adds proper constraints to the DDL",
            ),
        ]

        # Set up agent
        self.agent = self._create_agent()

    def _create_agent(self):
        """Create the DDL generation agent using updated LangChain approach"""

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
        """Generate initial DDL from schema with enhanced relationship handling"""

        chain = (
            PromptTemplate.from_template(
                get_base_ddl_prompt()
                + "\n\nGenerate ONLY the DDL statements for this schema:\n{schema}"
            )
            | self.llm
        )

        ddl = chain.invoke({"schema": self.formatted_json}).content
        self.ddl_script = ddl
        return ddl

    def validate_syntax(self, ddl):
        """Validate SQL syntax with detailed feedback"""

        chain = PromptTemplate.from_template(prompt) | self.llm
        return chain.invoke({"ddl": ddl}).content

    def optimize_for_oracle(self, ddl):
        """Optimize DDL for Oracle with specific enhancements"""

        chain = (
            PromptTemplate.from_template(
                get_validation_prompt() + "\n\nDDL to validate:\n{ddl}"
            )
            | self.llm
        )
        optimized_ddl = chain.invoke({"ddl": ddl}).content
        self.ddl_script = optimized_ddl
        return optimized_ddl

    def add_constraints(self, ddl):
        """Add constraints based on schema definition"""
        chain = (
            PromptTemplate.from_template(
                get_optimization_prompt()
                + "\n\nInput DDL:\n{ddl}\n\nOutput ONLY the optimized DDL with these changes applied."
            )
            | self.llm
        )
        constrained_ddl = chain.invoke(
            {"schema": self.formatted_json, "ddl": ddl}
        ).content
        self.ddl_script = constrained_ddl
        return constrained_ddl

    def generate_ddl(self):
        """Orchestrate the DDL generation process"""
        try:
            result = self.agent.invoke(
                {"schema": self.formatted_json, "tools": self.tools}
            )

            # Check if we got a FINAL_DDL response
            if isinstance(result, dict) and "output" in result:
                output = result["output"]
                if output.startswith("FINAL_DDL:"):
                    self.ddl_script = output.replace("FINAL_DDL:", "").strip()

            # Post-processing to ensure clean DDL
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
                "validation": f"Error during generation: {str(e)}",
                "status": "failed",
            }
