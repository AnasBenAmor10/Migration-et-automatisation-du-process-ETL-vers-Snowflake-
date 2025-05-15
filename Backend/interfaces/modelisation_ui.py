import gradio as gr
from controllers.modelisation_ctrl import toggle_oracle_params,lancer_modelisation

def create_modelisation_interface():
    with gr.Column(visible=False) as col_modelisation:
        gr.Markdown("## 📐 Options de Modélisation")
        
        # Aide et configuration (comme avant)
        with gr.Accordion("ℹ️ Aide - Comment remplir ces champs de la connexion Oracle?", open=True):
            gr.Markdown("""
            - **Schéma** : Nom du schéma Oracle à utiliser (ex : sales, hr)
            - **Utilisateur** : Identifiant de l'utilisateur Oracle
            - **Mot de passe** : Mot de passe associé à l'utilisateur
            - **SID** : Identifiant du système Oracle (ex : ORCL)
            - **Port** : Port d'écoute du serveur Oracle (par défaut : 1521)
            - **Host** : Adresse IP ou nom du serveur hébergeant la base Oracle
            """)
        
        source_type = gr.Dropdown(
            ["Oracle", "MySQL", "PostgreSQL", "SQL Server"], 
            label="Source de données 🗄️", 
            value="Oracle"
        )
        
        # Configuration Oracle
        with gr.Column(visible=True) as oracle_config:
            gr.Markdown("### 🔐 Configuration Oracle")
            with gr.Row():
                oracle_user = gr.Textbox(label="Utilisateur 👤", placeholder="ex: SYSTEM")
                oracle_password = gr.Textbox(label="Mot de passe 🔑", type="password")
            with gr.Row():
                oracle_host = gr.Textbox(label="Host 🌐", placeholder="ex: localhost ou 192.168.1.100")
                oracle_port = gr.Textbox(label="Port 🔌", value="1521")
            oracle_sid = gr.Textbox(label="SID 🏷️", placeholder="ex: ORCL")
        
        schema = gr.Textbox(label="Nom du schéma 📛", placeholder="ex: ventes")
        btn_lancer_model = gr.Button("🚀 Lancer la Modélisation", variant="primary")
        
        # Nouveaux éléments pour le suivi visuel
        with gr.Column():
            progress_bar = gr.Slider(minimum=0, maximum=1, step=0.01, label="Progression 📊", interactive=False)
            current_step = gr.Textbox(label="Étape actuelle 🚦", interactive=False)
            
            # Graphique des étapes
            with gr.Row():
                steps = ["Export", "Modélisation", "Génération DDL", "Exécution"]
                step_indicators = [
                    gr.Textbox(f"🔵 {step}", interactive=False, visible=False, 
                             elem_classes=["step-indicator"])
                    for step in steps
                ]
            
            output_model = gr.Textbox(label="📤 Détails d'exécution", interactive=False, lines=10)
        
        
        btn_lancer_model.click(
            fn=lancer_modelisation,
            inputs=[source_type, oracle_user, oracle_password, oracle_host, oracle_port, oracle_sid, schema],
            outputs=[output_model, progress_bar, current_step, *step_indicators],
            api_name="run_modelisation",
            concurrency_limit=1,
            queue=True
        )
        
        # Mise à jour dynamique des indicateurs d'étape
        def update_step_indicators(current_node):
            active_steps = {
                "Export Schema": 0,
                "Modelisation": 1,
                "Create DDL": 2,
                "Execute The DDL": 3
            }
            
            active_idx = active_steps.get(current_node, -1)
            
            updates = []
            for i, indicator in enumerate(step_indicators):
                visible = i <= active_idx
                emoji = "✅" if i < active_idx else "🔵" if i == active_idx else "⚪"
                text = f"{emoji} {steps[i]}"
                updates.append(gr.update(value=text, visible=visible))
            
            return updates
        
        current_step.change(
            fn=update_step_indicators,
            inputs=current_step,
            outputs=step_indicators
        )
        
        source_type.change(
            fn=toggle_oracle_params,
            inputs=source_type,
            outputs=oracle_config
        )
    
    return col_modelisation