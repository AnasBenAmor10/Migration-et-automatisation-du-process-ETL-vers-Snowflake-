import gradio as gr
import os
import time
import logging
from Agents.Data_Modelation.src.Model import Model, State
import queue
import threading
logger = logging.getLogger(__name__)

def modelisation_interface():
    return gr.update(visible=True), gr.update(value="🔍 Modélisation sélectionnée!")

def toggle_oracle_params(source):
    return gr.update(visible=source == "Oracle")

def lancer_modelisation(source, user, password, host, port, sid, schema):
    try:
        q = queue.Queue()

        def handle_callback(callback_data):
            """Capture les messages de progression et les met dans la file"""
            if isinstance(callback_data, dict):
                node_name = callback_data.get("current_node", "Inconnu")
                progress = callback_data.get("progress", 0)
                message = callback_data.get("message", "")
            else:
                node_name = "Erreur"
                progress = 0
                message = str(callback_data)

            step_states = {
                "Export Schema":     ("✅ Export", "🔵 Modélisation", "⚪ Génération DDL", "⚪ Exécution"),
                "Modelisation":      ("✅ Export", "✅ Modélisation", "🔵 Génération DDL", "⚪ Exécution"),
                "Create DDL":        ("✅ Export", "✅ Modélisation", "✅ Génération DDL", "🔵 Exécution"),
                "Execute The DDL":   ("✅ Export", "✅ Modélisation", "✅ Génération DDL", "✅ Exécution"),
                "Initialisation":    ("🔵 Export", "⚪ Modélisation", "⚪ Génération DDL", "⚪ Exécution"),
                "Erreur":            ("❌ Export", "❌ Modélisation", "❌ Génération DDL", "❌ Exécution"),
                "Terminé":           ("✅ Export", "✅ Modélisation", "✅ Génération DDL", "✅ Exécution")
            }

            steps = step_states.get(node_name, ("⚪ Export", "⚪ Modélisation", "⚪ Génération DDL", "⚪ Exécution"))
            q.put((message, progress, node_name, *steps)) 

        # Connecte le callback à Model
        Model.set_ui_callback(handle_callback)

        # Vérifie les paramètres Oracle
        if source == "Oracle":
            if not all([user, password, host, port, sid]):
                error_msg = "❌ Erreur: Tous les paramètres Oracle sont requis!"
                logger.error(error_msg)
                yield (error_msg, 0, "Erreur", "⚪ Export", "⚪ Modélisation", "⚪ Génération DDL", "⚪ Exécution")
                return
            os.environ["ORACLE_SOURCE"] = user
            os.environ["ORACLE_SOURCE_PASSWORD"] = password
            os.environ["ORACLE_HOST"] = host
            os.environ["ORACLE_PORT"] = port
            os.environ["ORACLE_SID"] = sid

        # État initial
        initial_state = State(
            Source_schema=None,
            New_Schema=None,
            error=None,
            status=None,
            ddl=None,
            schema_name=schema
        )

        # Premier message statique
        yield (
            "🔌 Initialisation du graphe de modélisation...",
            0.1,
            "Initialisation",
            "🔵 Export", "⚪ Modélisation", "⚪ Génération DDL", "⚪ Exécution"
        )

        # Container pour récupérer le résultat de manière sûre
        result_container = {}

        def run_model():
            try:
                result = Model.invoke(initial_state)
                result_container["result"] = result
            except Exception as e:
                result_container["result"] = {"error": str(e)}
            q.put("done")  # Signale la fin

        # Lancer dans un thread pour récupérer les callbacks pendant l'exécution
        t = threading.Thread(target=run_model)
        t.start()

        # Lire la queue et relayer les messages via yield
        while True:
            item = q.get()
            if item == "done":
                break
            yield item  # Renvoie au client / frontend

        # Fin de traitement : succès ou erreur
        result = result_container["result"]

        if result.get("error"):
            yield (
                f"❌ Erreur lors de la modélisation:\n{result['error']}",
                0,
                "Erreur",
                "❌ Export", "❌ Modélisation", "❌ Génération DDL", "❌ Exécution"
            )
        else:
            yield (
                f"✅ Modélisation terminée avec succès!\n\n📝 Statut: {result.get('status', 'N/A')}",
                1,
                "Terminé",
                "✅ Export", "✅ Modélisation", "✅ Génération DDL", "✅ Exécution"
            )

    except Exception as e:
        logger.exception("Erreur critique dans lancer_modelisation")
        yield (
            f"❌ Erreur critique: {str(e)}",
            0,
            "Erreur",
            "❌ Export", "❌ Modélisation", "❌ Génération DDL", "❌ Exécution"
        )
