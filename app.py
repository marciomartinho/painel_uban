# app.py
"""Aplicação Flask principal"""

from flask import Flask, render_template
import os
import sys

# Adiciona o diretório ao path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Importa módulos necessários
from app.modulos.formatacao import FormatadorMonetario
from app.modulos.periodo import obter_periodo_referencia

def create_app():
    """Factory para criar a aplicação Flask"""
    app = Flask(__name__, 
                template_folder='templates',
                static_folder='static')
    
    # Configurações
    app.config['SECRET_KEY'] = 'chave-secreta-desenvolvimento'
    app.config['DEBUG'] = True
    
    # Registra filtros e funções globais
    @app.context_processor
    def utility_processor():
        """Disponibiliza utilitários em todos os templates"""
        return dict(
            fmt=FormatadorMonetario,
            obter_periodo_referencia=obter_periodo_referencia
        )
    
    # Rota principal
    @app.route('/')
    def index():
        periodo = obter_periodo_referencia()
        return render_template('index.html', periodo=periodo)
    
    # Registra blueprints
    from app.routes_relatorios import relatorios_bp
    app.register_blueprint(relatorios_bp)
    
    return app

# Cria a aplicação
app = create_app()

if __name__ == '__main__':
    print("=" * 60)
    print("Sistema de Relatórios Orçamentários")
    print("=" * 60)
    print("Acesse: http://localhost:5000")
    print("=" * 60)
    
    app.run(host='0.0.0.0', port=5000, debug=True)