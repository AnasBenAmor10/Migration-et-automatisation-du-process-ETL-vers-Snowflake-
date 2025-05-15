from interfaces.main_interface import create_main_interface
import gradio as gr

def main():
    app = create_main_interface()
    app.launch()

if __name__ == "__main__":
    main()