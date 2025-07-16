# app/__init__.py

from flask import Flask

def create_app():
    """Cria e configura uma instância da aplicação Flask."""
    app = Flask(__name__)

    with app.app_context():
        # Importa e registra o blueprint do relatório de inconsistências
        from .relatorios.routes_inconsistencias import inconsistencias_bp
        app.register_blueprint(inconsistencias_bp)

        # Se houver outras partes da aplicação, registre-as aqui também.
        
    return app