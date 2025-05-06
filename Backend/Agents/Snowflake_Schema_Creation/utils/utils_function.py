import logging
import smtplib
from email.message import EmailMessage
import os
from dotenv import load_dotenv
from langchain_groq import ChatGroq
from langchain.schema import HumanMessage, SystemMessage
import re
from Prompt import *

# Chargement des variables d'environnement
load_dotenv()

# Configuration du logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def analyze_error(error_msg: str) -> str:
    try:
        llm = ChatGroq(
            temperature=0.3,
            groq_api_key=os.getenv("groq_api"),
            model_name=os.getenv("MODEL"),
        )

        messages = [
            SystemMessage(
                content=(
                    "You are a senior cloud database engineer specializing in Snowflake, assisting in a pipeline that migrates database schemas into Snowflake. "
                    "When given an error message, respond in this exact format:\n\n"
                    "## Error\n<Error message>\n\n"
                    "## Explain the Error\nYou have an error in <specific part> because <technical cause>.\n\n"
                    "## How to Fix it (Recommendation)\n<clear recommended fix>.\n\n"
                    "Focus on Snowflake syntax and behavior. Be precise, technical, and Explain with details"
                )
            ),
            HumanMessage(content=f"Error: {error_msg}"),
        ]
        response = llm.invoke(messages)
        return response.content
    except Exception as e:
        return f"Error analysis unavailable: {str(e)}"


# --- Fonction d'envoi d'email ---
def send_error_email(error_msg: str, analysis: str):
    """Sends an email with the error and its analysis."""
    try:
        msg = EmailMessage()
        msg["Subject"] = "🚨 Error in DDL workflow"
        msg["From"] = os.getenv("SMTP_USER")
        msg["To"] = os.getenv("EMAIL_TO")
        # Regex pattern
        pattern = r"## Error\n(.*?)\n\n## Explain the Error\n(.*?)\n\n## How to Fix it \(Recommendation\)\n(.*)"
        # Match and extract
        match = re.search(pattern, analysis, re.DOTALL)
        if match:
            error_part = match.group(1).strip()
            explain_part = match.group(2).strip()
            fix_part = match.group(3).strip()
        # Build HTML content
        email_content = f"""
        <html>
        <body>
            <p><strong>Dear Team,</strong></p>

            <p>An error occurred in the <strong>Snowflake Migration Pipeline</strong> process for <strong>HR DATABASE</strong>. Please find the details below:</p>

            <h3>🚫 Original Error</h3>
            <pre style="background-color:#f8f8f8;padding:10px;border-radius:5px;border:1px solid #ddd;">{error_msg}</pre>

            <h3>🔎 Technical Analysis</h3>
            <p>{explain_part}</p>

            <h3>✅ Recommended Fix</h3>
            <pre style="background-color:#f0fff0;padding:10px;border-radius:5px;border:1px solid #cce5cc;">{fix_part}</pre>

            <br>
            <p>Best regards,<br>
            <strong>Automation Migration System For Talan Tunisia</strong><br>
            IT Operations Team</p>
        </body>
        </html>
        """

        msg.set_content("This is a multipart message in HTML format.", subtype="plain")
        msg.add_alternative(email_content, subtype="html")

        with smtplib.SMTP(os.getenv("SMTP_HOST"), int(os.getenv("SMTP_PORT"))) as s:
            s.ehlo()
            s.starttls()
            s.ehlo()
            s.login(os.getenv("SMTP_USER"), os.getenv("SMTP_PASSWORD"))
            s.send_message(msg)

        logger.info("Email d'erreur envoyé avec succès")

    except Exception as e:
        logger.error(f"Échec envoi email: {str(e)}")


