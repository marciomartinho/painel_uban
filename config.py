# config.py - Configuração de banco de dados
import os

class Config:
    # Detecta se está no Railway (produção) ou local
    if os.environ.get('RAILWAY_ENVIRONMENT') or os.environ.get('DATABASE_URL'):
        # Produção - PostgreSQL no Railway (todos os dados em um banco)
        DATABASE_URL = os.environ.get('DATABASE_URL', 'postgresql://postgres:uzVUcFKomVccdwGGtwrGeyOHWcrjxiIu@postgres.railway.internal:5432/railway')
        DATABASE_TYPE = 'postgresql'
        print("🚀 Usando PostgreSQL (Railway)")
    else:
        # Local - SQLite (3 bancos separados)
        DATABASE_TYPE = 'sqlite'
        BASE_PATH = os.path.join(os.path.dirname(__file__), 'dados/db')
        BANCOS = {
            'saldos': os.path.join(BASE_PATH, 'banco_saldo_receita.db'),
            'lancamentos': os.path.join(BASE_PATH, 'banco_lancamento_receita.db'),
            'dimensoes': os.path.join(BASE_PATH, 'banco_dimensoes.db')
        }
        print("💻 Usando SQLite (Local - 3 bancos)")

# Para debug
def get_config_info():
    """Retorna informações sobre a configuração atual"""
    config = Config()
    return {
        'database_type': config.DATABASE_TYPE,
        'is_railway': bool(os.environ.get('RAILWAY_ENVIRONMENT')),
        'has_database_url': bool(os.environ.get('DATABASE_URL'))
    }