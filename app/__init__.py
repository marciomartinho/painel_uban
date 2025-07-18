# app/__init__.py

from flask import Flask

def create_app():
    """Cria e configura uma instância da aplicação Flask."""
    app = Flask(__name__)

    with app.app_context():
        # Importa e registra o blueprint do relatório de inconsistências
        from .routes_inconsistencias import inconsistencias_bp
        app.register_blueprint(inconsistencias_bp)

        # Se houver outras partes da aplicação, registre-as aqui também.
        # Por exemplo, se quiser registrar os outros blueprints aqui em vez de no main.py:
        # from .routes_relatorios import relatorios_bp
        # from .routes_visualizador import visualizador_bp
        # app.register_blueprint(relatorios_bp)
        # app.register_blueprint(visualizador_bp)

    return app