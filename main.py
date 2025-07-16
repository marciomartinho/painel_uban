# main.py (arquivo principal da aplicação)
"""
Arquivo principal da aplicação Flask.
"""

from flask import Flask, render_template
from app.modulos.periodo import obter_periodo_referencia

# Importação dos blueprints existentes
from app.routes_relatorios import relatorios_bp
from app.routes_visualizador import visualizador_bp

# <<< MUDANÇA >>>
# O caminho foi corrigido para refletir que o arquivo está em 'app/' e não em 'app/relatorios/'
from app.routes_inconsistencias import inconsistencias_bp
import os

# Cria a aplicação Flask
app = Flask(__name__,
            template_folder='app/templates',
            static_folder='app/static')

# Configurações
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'dev-secret-key-change-in-production')

# Registra os blueprints
app.register_blueprint(relatorios_bp)
app.register_blueprint(visualizador_bp)
app.register_blueprint(inconsistencias_bp)


# Adiciona a função obter_periodo_referencia ao contexto dos templates
@app.context_processor
def inject_periodo():
    return {'obter_periodo_referencia': obter_periodo_referencia}

# Rota principal
@app.route('/')
def index():
    periodo = obter_periodo_referencia()
    return render_template('index.html', periodo=periodo)

# Tratamento de erros
@app.errorhandler(404)
def page_not_found(e):
    return render_template('erro.html', mensagem='Página não encontrada'), 404

@app.errorhandler(500)
def internal_error(e):
    return render_template('erro.html', mensagem='Erro interno do servidor'), 500

if __name__ == '__main__':
    # Modo de desenvolvimento
    app.run(debug=True, host='0.0.0.0', port=5000)