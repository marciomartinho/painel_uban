# app/__init__.py

from flask import Flask, render_template
from .modulos.periodo import obter_periodo_referencia
import os

def create_app():
    """Cria e configura uma instância da aplicação Flask."""
    app = Flask(__name__,
                template_folder='templates',
                static_folder='static')

    # Configurações da aplicação
    app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'dev-secret-key')

    # Importa e registra TODOS os blueprints
    from .routes_relatorios import relatorios_bp
    from .routes_visualizador import visualizador_bp
    from .routes_inconsistencias import inconsistencias_bp
    app.register_blueprint(relatorios_bp)
    app.register_blueprint(visualizador_bp)
    app.register_blueprint(inconsistencias_bp)

    # Adiciona a função obter_periodo_referencia ao contexto dos templates
    @app.context_processor
    def inject_periodo():
        return {'obter_periodo_referencia': obter_periodo_referencia}

    # Define a rota principal AQUI
    @app.route('/')
    def index():
        periodo = obter_periodo_referencia()
        return render_template('index.html', periodo=periodo)

    # Tratamento de erros
    @app.errorhandler(404)
    def page_not_found(e):
        return render_template('erro.html', mensagem='Página não encontrada.'), 404

    @app.errorhandler(500)
    def internal_error(e):
        return render_template('erro.html', mensagem='Ocorreu um erro interno no servidor.'), 500

    return app