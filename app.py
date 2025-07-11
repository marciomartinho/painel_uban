def create_app():
    """Factory para criar a aplicação Flask"""
    # Define os caminhos corretos para templates e static
    template_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')
    static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static')
    
    # Se as pastas estiverem em app/, ajusta o caminho
    if not os.path.exists(template_dir):
        template_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'app', 'templates')
        static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'app', 'static')
    
    app = Flask(__name__, 
                template_folder=template_dir,
                static_folder=static_dir)