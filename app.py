# app.py
"""Aplicação Flask principal"""

from flask import Flask, render_template
import os
import sys
import time

# Adiciona o diretório ao path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Importa módulos necessários
from app.modulos.formatacao import FormatadorMonetario
from app.modulos.periodo import obter_periodo_referencia

def create_app():
    """Factory para criar a aplicação Flask"""
    # Define os caminhos corretos para templates e static
    base_dir = os.path.dirname(os.path.abspath(__file__))
    template_dir = os.path.join(base_dir, 'app', 'templates')
    static_dir = os.path.join(base_dir, 'app', 'static')

    app = Flask(__name__,
                template_folder=template_dir,
                static_folder=static_dir)

    # Configurações
    # Em produção, a SECRET_KEY deveria vir de uma variável de ambiente
    app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'chave-secreta-padrao')
    # O modo DEBUG deve ser False em produção
    app.config['DEBUG'] = False

    # Registra filtros e funções globais
    @app.context_processor
    def utility_processor():
        """Disponibiliza utilitários em todos os templates"""
        return dict(
            fmt=FormatadorMonetario,
            obter_periodo_referencia=obter_periodo_referencia,
            # Adiciona timestamp para cache busting
            cache_buster=int(time.time())
        )

    # Rota principal
    @app.route('/')
    def index():
        try:
            periodo = obter_periodo_referencia()
            return render_template('index.html', periodo=periodo)
        except Exception as e:
            # Em produção, é melhor logar o erro do que mostrá-lo ao usuário
            print(f"Erro ao renderizar index.html: {e}")
            # Retorna uma página de erro genérica
            return "<h1>Ocorreu um erro ao carregar a página.</h1>", 500

    # Registra blueprints
    try:
        from app.routes_relatorios import relatorios_bp
        from app.routes_visualizador import visualizador_bp
        app.register_blueprint(relatorios_bp)
        app.register_blueprint(visualizador_bp)
    except ImportError as e:
        print(f"Erro ao importar blueprints: {e}")

    return app

# Cria a aplicação para que o Gunicorn possa encontrá-la
app = create_app()

# O BLOCO "if __name__ == '__main__':" FOI REMOVIDO DAQUI