# main.py
"""Aplicação Flask principal - Versão híbrida"""

from flask import Flask, render_template
import os
import sys
import time

# Adiciona o diretório ao path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Importa módulos necessários
try:
    from app.modulos.formatacao import FormatadorMonetario
    from app.modulos.periodo import obter_periodo_referencia
except ImportError as e:
    print(f"Erro ao importar módulos: {e}")
    # Fallback básico se os módulos não existirem
    class FormatadorMonetario:
        @staticmethod
        def formatar_valor(valor):
            return f"R$ {valor:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
    
    def obter_periodo_referencia():
        return {
            'mes': 6,
            'ano': 2025,
            'mes_nome': 'Junho',
            'periodo_completo': 'Junho/2025'
        }

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
    
    # Configurações baseadas no ambiente
    if os.environ.get('RAILWAY_ENVIRONMENT') or os.environ.get('DATABASE_URL'):
        # Produção
        app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'railway-production-key')
        app.config['DEBUG'] = False
        print("🚀 Executando em PRODUÇÃO (Railway)")
    else:
        # Desenvolvimento
        app.config['SECRET_KEY'] = 'chave-secreta-desenvolvimento'
        app.config['DEBUG'] = True
        print("💻 Executando em DESENVOLVIMENTO (Local)")
    
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
        if not app.config['DEBUG']:
            return response
            
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
            ambiente = "Railway (PostgreSQL)" if (os.environ.get('RAILWAY_ENVIRONMENT') or os.environ.get('DATABASE_URL')) else "Local (SQLite)"
            
            return render_template('index.html', 
                                 periodo=periodo, 
                                 ambiente=ambiente)
        except Exception as e:
            print(f"Erro ao renderizar index.html: {e}")
            # Retorna uma página de erro simples
            ambiente = "Railway (PostgreSQL)" if (os.environ.get('RAILWAY_ENVIRONMENT') or os.environ.get('DATABASE_URL')) else "Local (SQLite)"
            
            return f"""
            <h1>Sistema de Relatórios Orçamentários</h1>
            <h2>Ambiente: {ambiente}</h2>
            <h3>Status: ✅ Aplicação funcionando!</h3>
            <p><strong>Erro ao carregar template:</strong> {str(e)}</p>
            <p>Verifique se o arquivo 'app/templates/index.html' existe.</p>
            <hr>
            <p><em>Período de referência:</em> Junho/2025 (padrão)</p>
            """
    
    # Rota de teste de banco
    @app.route('/test-db')
    def test_db():
        """Rota para testar conexão com banco de dados"""
        try:
            from config import get_config_info
            from database import test_connection
            
            config_info = get_config_info()
            db_success = test_connection()
            
            return f"""
            <h1>Teste de Banco de Dados</h1>
            <h2>Configuração:</h2>
            <ul>
                <li>Tipo de banco: {config_info['database_type']}</li>
                <li>Railway: {config_info['is_railway']}</li>
                <li>DATABASE_URL: {config_info['has_database_url']}</li>
            </ul>
            <h2>Conexão:</h2>
            <p>Status: {'✅ Sucesso' if db_success else '❌ Falhou'}</p>
            """
        except Exception as e:
            return f"""
            <h1>Teste de Banco de Dados</h1>
            <p>❌ Erro: {str(e)}</p>
            <p>Módulos de banco não encontrados ou erro na configuração.</p>
            """
    
    # Registra blueprints (se existirem)
    try:
        from app.routes_relatorios import relatorios_bp
        from app.routes_visualizador import visualizador_bp
        app.register_blueprint(relatorios_bp)
        app.register_blueprint(visualizador_bp)
        print("✅ Blueprints carregados com sucesso")
    except ImportError as e:
        print(f"⚠️  Blueprints não encontrados: {e}")
    
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
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        .ambiente { background: #e3f2fd; padding: 10px; border-radius: 5px; margin: 20px 0; }
        .success { color: #4caf50; }
        .info { color: #2196f3; }
    </style>
</head>
<body>
    <h1>🏛️ Sistema de Relatórios Orçamentários</h1>
    
    <div class="ambiente">
        <h3>📊 Ambiente: {{ ambiente }}</h3>
    </div>
    
    {% if periodo %}
    <div class="info">
        <h3>📅 Período de Referência</h3>
        <p><strong>{{ periodo.periodo_completo }}</strong></p>
        <ul>
            <li>Mês: {{ periodo.mes }}</li>
            <li>Ano: {{ periodo.ano }}</li>
            <li>Nome: {{ periodo.mes_nome }}</li>
        </ul>
    </div>
    {% endif %}
    
    <div class="success">
        <h3>✅ Sistema Online!</h3>
        <p>A aplicação está funcionando corretamente em modo híbrido.</p>
    </div>
    
    <h3>🔗 Links de Teste:</h3>
    <ul>
        <li><a href="/test-db">🗄️ Testar Banco de Dados</a></li>
    </ul>
</body>
</html>""")
    
    # Obtém a porta do ambiente (Railway define automaticamente)
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=app.config['DEBUG'])