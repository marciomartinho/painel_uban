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
    
    # Verifica se as pastas existem
    if not os.path.exists(template_dir):
        print(f"ERRO: Pasta de templates não encontrada em: {template_dir}")
        # Tenta criar a estrutura
        os.makedirs(template_dir, exist_ok=True)
        os.makedirs(os.path.join(template_dir, 'relatorios'), exist_ok=True)
        os.makedirs(os.path.join(template_dir, 'componentes'), exist_ok=True)
    
    if not os.path.exists(static_dir):
        print(f"ERRO: Pasta static não encontrada em: {static_dir}")
        # Tenta criar a estrutura
        os.makedirs(static_dir, exist_ok=True)
        os.makedirs(os.path.join(static_dir, 'css'), exist_ok=True)
        os.makedirs(os.path.join(static_dir, 'js'), exist_ok=True)
    
    print(f"Template folder: {template_dir}")
    print(f"Static folder: {static_dir}")
    
    app = Flask(__name__, 
                template_folder=template_dir,
                static_folder=static_dir)
    
    # Configurações
    app.config['SECRET_KEY'] = 'chave-secreta-desenvolvimento'
    app.config['DEBUG'] = True
    # Desabilita cache de arquivos estáticos em desenvolvimento
    app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0
    
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
    
    # Adiciona headers para prevenir cache
    @app.after_request
    def add_header(response):
        """Adiciona headers para evitar cache em desenvolvimento"""
        if 'Cache-Control' not in response.headers:
            response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, post-check=0, pre-check=0, max-age=0'
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '-1'
        return response
    
    # Rota principal
    @app.route('/')
    def index():
        try:
            periodo = obter_periodo_referencia()
            return render_template('index.html', periodo=periodo)
        except Exception as e:
            print(f"Erro ao renderizar index.html: {e}")
            # Retorna uma página de erro simples
            return f"""
            <h1>Erro ao carregar a página</h1>
            <p>{str(e)}</p>
            <p>Verifique se o arquivo 'app/templates/index.html' existe.</p>
            """
    
    # Registra blueprints
    try:
        from app.routes_relatorios import relatorios_bp
        from app.routes_visualizador import visualizador_bp
        app.register_blueprint(relatorios_bp)
        app.register_blueprint(visualizador_bp)
    except ImportError as e:
        print(f"Erro ao importar blueprints: {e}")
    
    return app

# Cria a aplicação FORA da condição if __name__ == '__main__'
# Isso permite que o Gunicorn acesse a instância
app = create_app()

if __name__ == '__main__':
    print("=" * 60)
    print("Sistema de Relatórios Orçamentários")
    print("=" * 60)
    print("Acesse: http://localhost:5000")
    print("=" * 60)
    
    # Verifica se os arquivos existem antes de iniciar
    base_dir = os.path.dirname(os.path.abspath(__file__))
    index_path = os.path.join(base_dir, 'app', 'templates', 'index.html')
    if not os.path.exists(index_path):
        print(f"AVISO: Arquivo index.html não encontrado em: {index_path}")
        print("Criando arquivo index.html de exemplo...")
        
        # Cria um index.html básico se não existir
        os.makedirs(os.path.dirname(index_path), exist_ok=True)
        with open(index_path, 'w', encoding='utf-8') as f:
            f.write("""<!DOCTYPE html>
<html>
<head>
    <title>Sistema de Relatórios</title>
</head>
<body>
    <h1>Sistema de Relatórios Orçamentários</h1>
    <p>Bem-vindo! Configure os templates corretamente.</p>
    {% if periodo %}
    <p>Período: {{ periodo.periodo_completo }}</p>
    {% endif %}
</body>
</html>""")
    
    app.run(host='0.0.0.0', port=5000, debug=True)