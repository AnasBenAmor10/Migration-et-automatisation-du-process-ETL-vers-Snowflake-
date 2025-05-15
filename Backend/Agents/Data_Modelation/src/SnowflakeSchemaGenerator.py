from Agents.Data_Modelation.src.utils.Prompt import snowflake_prompt_template1
import json
from langchain_google_genai import ChatGoogleGenerativeAI
from Agents.Data_Modelation.src.utils.Config import GEMINI_API_KEY
import re


class SnowflakeSchemaGenerator:
    """
    Une classe pour générer des schémas Snowflake à partir de schémas d'entrée
    en utilisant le modèle Gemini de Google Generative AI.
    """

    def __init__(self):
        """Initialise le modèle Gemini avec les paramètres configurés."""
        self.model = ChatGoogleGenerativeAI(
            model="gemini-2.0-flash-lite",
            google_api_key=GEMINI_API_KEY,
            temperature=0,
            max_tokens=None,
            timeout=None,
            max_retries=2,
        )

    def transform_schema_to_snowflake(self, schema):
        """
        Transforme un schéma JSON en format Snowflake en utilisant un prompt template.

        Args:
            schema (dict): Le schéma d'entrée sous forme de dictionnaire

        Returns:
            str: Le schéma transformé en format Snowflake
        """
        # Formatte le prompt avec le schéma JSON indenté
        formatted_prompt = snowflake_prompt_template1.format(
            schema_json=json.dumps(schema, indent=2)
        )

        # Appelle le modèle pour générer la réponse
        response = self.model.invoke(formatted_prompt)

        # Extrait le contenu de la réponse
        return response.content if hasattr(response, "content") else str(response)

    def generate_model(self, input_schema):
        """
        Génère un modèle Snowflake à partir d'un schéma d'entrée.

        Args:
            input_schema (dict): Le schéma d'entrée à transformer

        Returns:
            str: Le modèle Snowflake généré
        """
        text = self.transform_schema_to_snowflake(input_schema)

        # Use regex to extract the JSON content between the outermost curly braces
        pattern = r"\{.*\}"
        match = re.search(pattern, text, re.DOTALL)

        if match:
            json_str = match.group(0)
            try:
                # Parse the JSON string into a Python dictionary
                json_data = json.loads(json_str)
                # Convert back to JSON string with proper formatting if needed
                formatted_json = json.dumps(json_data, indent=2)
                return formatted_json
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON: {e}")
        else:
            print("No JSON content found in the text output.")
