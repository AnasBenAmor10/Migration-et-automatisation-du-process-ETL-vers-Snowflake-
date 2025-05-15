import gradio as gr

def create_migration_interface():
    with gr.Column(visible=False) as col_migration:
        gr.Markdown("## ❄️ Options de Migration vers Snowflake")
        with gr.Accordion("ℹ️ Aide - Configuration requise", open=False):
            gr.Markdown("""
            - **Type de source**: Sélectionnez votre SGBD source
            - **Host**: Adresse du serveur (ex: localhost, 192.168.1.100)
            - **Database**: Nom de la base à migrer
            - **Identifiants**: Accès avec les droits de lecture
            """)
        
        source_type = gr.Dropdown(["MySQL", "PostgreSQL", "Oracle", "SQL Server"], 
                                label="Type de source 🗄️", value="MySQL")
        
        with gr.Row():
            host = gr.Textbox(label="Host 🌐", placeholder="ex: localhost ou 192.168.1.100")
            database = gr.Textbox(label="Nom de la base de données 💾", placeholder="ex: ma_base")
        
        with gr.Row():
            user = gr.Textbox(label="Utilisateur 👤", placeholder="ex: admin")
            password = gr.Textbox(label="Mot de passe 🔑", type="password")
        
        btn_lancer_migr = gr.Button("🚀 Lancer la Migration", variant="primary")
        output_migr = gr.Textbox(label="📤 Résultat", interactive=False, lines=7)
        
        # Retourner à la fois le conteneur et les inputs nécessaires
        inputs = [source_type, host, database, user, password]
        return col_migration, inputs