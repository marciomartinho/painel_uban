import os
from flask import Flask
from .modulos.formatacao import formatar_moeda, formatar_percentual
from config import Config

def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)

    # Registra os filtros de template
    app.jinja_env.filters['formatar_moeda'] = formatar_moeda
    app.jinja_env.filters['formatar_percentual'] = formatar_percentual

    # --- INÍCIO DA CORREÇÃO ---
    # Importa e registra os blueprints
    from .routes_main import main_bp           # Importa a nova rota principal
    from .routes_visualizador import visualizador_bp
    from .routes_relatorios import relatorios_bp
    from .routes_inconsistencias import inconsistencias_bp
    from .routes_RREO import rreo_bp
    
    app.register_blueprint(main_bp)           # Registra a rota principal em '/'
    app.register_blueprint(visualizador_bp, url_prefix='/visualizador')
    app.register_blueprint(relatorios_bp, url_prefix='/relatorios')
    app.register_blueprint(inconsistencias_bp, url_prefix='/inconsistencias')
    app.register_blueprint(rreo_bp, url_prefix='/rreo')
    # --- FIM DA CORREÇÃO ---

    return app