# debug_endpoints.py
"""
Script para verificar os endpoints registrados na aplicação
Execute este arquivo para diagnosticar o problema
"""

from flask import Flask
from app.routes_relatorios import relatorios_bp
from app.routes_visualizador import visualizador_bp

app = Flask(__name__, 
            template_folder='app/templates',
            static_folder='app/static')

# Registra os blueprints
app.register_blueprint(relatorios_bp)
app.register_blueprint(visualizador_bp)

# Rota principal
@app.route('/')
def index():
    return "Index OK"

# Lista todos os endpoints
print("=== ENDPOINTS REGISTRADOS ===")
for rule in app.url_map.iter_rules():
    print(f"{rule.endpoint}: {rule.rule}")

# Verifica especificamente o endpoint problemático
print("\n=== VERIFICAÇÃO ESPECÍFICA ===")
try:
    with app.test_request_context():
        from flask import url_for
        print("url_for('index'):", url_for('index'))
        print("url_for('relatorios.index'):", url_for('relatorios.index'))
except Exception as e:
    print(f"Erro ao acessar 'relatorios.index': {e}")

print("\n=== BLUEPRINTS REGISTRADOS ===")
for blueprint_name in app.blueprints:
    print(f"Blueprint: {blueprint_name}")
    bp = app.blueprints[blueprint_name]
    print(f"  URL prefix: {bp.url_prefix}")