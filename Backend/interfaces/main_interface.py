import gradio as gr
from interfaces.modelisation_ui import create_modelisation_interface
# from interfaces.migration_ui import create_migration_interface
from controllers.modelisation_ctrl import modelisation_interface, toggle_oracle_params
# from controllers.migration_ctrl import migration_interface

def create_main_interface():
    with gr.Blocks(title="Outil de Base de Données", theme="soft", css="""
        .step-indicator {
            text-align: center;
            padding: 8px;
            border-radius: 5px;
            margin: 5px;
            font-weight: bold;
        }
        .success-box {
            background-color: #e6f7e6;
            padding: 10px;
            border-radius: 5px;
            border-left: 4px solid #4CAF50;
        }
        .error-box {
            background-color: #ffebee;
            padding: 10px;
            border-radius: 5px;
            border-left: 4px solid #F44336;
        }
        """) as app:
        # En-tête
        gr.Markdown("# 🗃️ Outil de Base de Données")
        gr.Markdown("Choisissez une option ci-dessous 👇")
        
        # Composants communs
        status = gr.Textbox(label="Statut", interactive=False, value="⏳ En attente de sélection...")
        
        # Boutons de navigation
        with gr.Row():
            btn_modelisation = gr.Button("📐 Modélisation de Base", variant="primary")
            btn_migration = gr.Button("❄️ Migration vers Snowflake", variant="primary")
        
        # Interfaces
        col_modelisation = create_modelisation_interface()
        # col_migration = create_migration_interface()
        
        # Gestion des événements
        btn_modelisation.click(
            fn=modelisation_interface,
            outputs=[col_modelisation, status]
        )
        
        # btn_migration.click(
        #     fn=migration_interface,
        #     outputs=[col_modelisation, col_migration, status]
        # )
    
    return app