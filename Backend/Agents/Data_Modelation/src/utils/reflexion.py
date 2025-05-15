
class ModelReflectionAgent:
    def __init__(self):
        # Initialize two different LLM instances with different roles
        self.modeler_llm = "ChatOpenAI(model="gpt-4", temperature=0.7)"
        self.critic_llm = "ChatOpenAI(model="gpt-4", temperature=0.3)"
        
        # Define the modeling prompt
        self.modeling_prompt = ChatPromptTemplate.from_template(
            """You are a expert data modeler specializing in schema conversion from Oracle to Snowflake.
            
            Source Schema:
            {source_schema}
            
            Generate an optimized Snowflake schema considering:
            1. Snowflake best practices
            2. Performance optimization
            3. Cost efficiency
            4. Data integrity
            
            Return the schema in JSON format."""
        )
        
        # Define the critique prompt
        self.critique_prompt = ChatPromptTemplate.from_template(
            """You are a rigorous schema quality assurance expert. Analyze this Snowflake schema:
            
            {current_model}
            
            Source Schema:
            {source_schema}
            
            Provide detailed feedback on:
            1. Any missing elements from source
            2. Snowflake anti-patterns
            3. Potential performance issues
            4. Cost optimization opportunities
            5. Data type mismatches
            
            Be specific and suggest concrete improvements."""
        )
        
        # Define the refinement prompt
        self.refinement_prompt = ChatPromptTemplate.from_template(
            """You are a data modeling expert. Refine this schema based on the critique:
            
            Current Model:
            {current_model}
            
            Critique:
            {critique}
            
            Generate an improved version addressing all valid points from the critique.
            Maintain a record of changes made."""
        )

    def generate_initial_model(self, source_schema: Dict) -> Dict:
        """Generate the first version of the model"""
        chain = self.modeling_prompt | self.modeler_llm | StrOutputParser()
        result = chain.invoke({"source_schema": source_schema})
        return json.loads(result)

    def critique_model(self, source_schema: Dict, current_model: Dict) -> str:
        """Generate critique of the current model"""
        chain = self.critique_prompt | self.critic_llm | StrOutputParser()
        return chain.invoke({"source_schema": source_schema, "current_model": current_model})

    def refine_model(self, current_model: Dict, critique: str) -> Dict:
        """Refine the model based on critique"""
        chain = self.refinement_prompt | self.modeler_llm | StrOutputParser()
        result = chain.invoke({"current_model": current_model, "critique": critique})
        return json.loads(result)