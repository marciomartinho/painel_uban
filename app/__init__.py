import os
from flask import Flask
from .modulos.formatacao import formatar_moeda, formatar_percentual
from config import Config # <-- MUDANÇA 1: Importa a classe Config diretamente

def create_app():
    app = Flask(__name__)

    # MUDANÇA 2: Carrega as configurações diretamente da sua classe Config
    app.config.from_object(Config)

    # Registra os filtros de template
    app.jinja_env.filters['formatar_moeda'] = formatar_moeda
    app.jinja_env.filters['formatar_percentual'] = formatar_percentual

    # Importa e registra os blueprints
    from .routes_visualizador import visualizador_bp
    from .routes_relatorios import relatorios_bp
    from .routes_inconsistencias import inconsistencias_bp
    from .routes_RREO import rreo_bp
    
    app.register_blueprint(visualizador_bp)
    app.register_blueprint(relatorios_bp)
    app.register_blueprint(inconsistencias_bp)
    app.register_blueprint(rreo_bp)

    return app