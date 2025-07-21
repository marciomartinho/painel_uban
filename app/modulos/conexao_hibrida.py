# app/modulos/conexao_hibrida.py
"""
Sistema de conexão híbrido - SQLite local + PostgreSQL produção
Ponto central e ÚNICA fonte de verdade para conexões.
"""

import os
import sqlite3
import psycopg2
import psycopg2.extras

def get_db_environment():
    """Verifica se está em produção (Railway/Postgres) ou local (SQLite)."""
    if os.environ.get('RAILWAY_ENVIRONMENT') or os.environ.get('DATABASE_URL'):
        return 'postgres'
    else:
        return 'sqlite'

def adaptar_query(query: str) -> str:
    """
    Adapta a query para o ambiente de banco de dados correto.
    """
    if get_db_environment() == 'postgres':
        # Para Postgres, só precisamos trocar o placeholder e remover o alias do SQLite
        query = query.replace('?', '%s')
        query = query.replace('lancamentos_db.', '')
        # ADICIONE ESTA LINHA para remover prefixos de despesa também
        query = query.replace('lancamentos_despesa_db.', '')
        query = query.replace('saldos_despesa_db.', '')
    # Para SQLite, a query original com "dimensoes." e "?" já funciona
    return query

class ConexaoBanco:
    """Gerenciador de contexto para garantir que a conexão seja sempre fechada."""

    def __init__(self, db_name='saldos'):
        self.conn = None
        self.env = get_db_environment()
        self.db_name = db_name

    def __enter__(self):
        if self.env == 'postgres':
            try:
                database_url = os.environ.get('DATABASE_URL')
                if not database_url:
                    raise ConnectionError("Variável de ambiente DATABASE_URL não encontrada.")
                
                if database_url.startswith("postgres://"):
                    database_url = database_url.replace("postgres://", "postgresql://", 1)
                    
                self.conn = psycopg2.connect(database_url)
            except Exception as e:
                print(f"Erro fatal ao conectar ao PostgreSQL: {e}")
                raise
        else: # sqlite
            try:
                project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
                base_path = os.path.join(project_root, 'dados', 'db')
                
                # ATUALIZAÇÃO: Inclui os novos bancos de despesa
                db_files = {
                    'saldos': 'banco_saldo_receita.db',
                    'lancamentos': 'banco_lancamento_receita.db',
                    'dimensoes': 'banco_dimensoes.db',
                    'saldos_despesa': 'banco_saldo_despesa.db',         # NOVO
                    'lancamentos_despesa': 'banco_lancamento_despesa.db' # NOVO
                }
                db_filename = db_files.get(self.db_name, 'banco_saldo_receita.db')
                caminho_principal = os.path.join(base_path, db_filename)

                if not os.path.exists(caminho_principal):
                    raise FileNotFoundError(f"Banco de dados '{self.db_name}' não encontrado: {caminho_principal}")

                self.conn = sqlite3.connect(caminho_principal)
                self.conn.row_factory = sqlite3.Row
                
                # ATUALIZAÇÃO: Lógica de anexação para cada banco
                if self.db_name == 'saldos':
                    bancos_anexar = {
                        'dimensoes': 'banco_dimensoes.db',
                        'lancamentos_db': 'banco_lancamento_receita.db'
                    }
                elif self.db_name == 'saldos_despesa':
                    # Para saldos de despesa, anexa dimensões
                    bancos_anexar = {
                        'dimensoes': 'banco_dimensoes.db',
                        'lancamentos_despesa_db': 'banco_lancamento_despesa.db'
                    }
                elif self.db_name == 'lancamentos_despesa':
                    # Para lançamentos de despesa, anexa dimensões
                    bancos_anexar = {
                        'dimensoes': 'banco_dimensoes.db'
                    }
                else:
                    bancos_anexar = {}
                
                for alias, db_file in bancos_anexar.items():
                    caminho_anexo = os.path.join(base_path, db_file)
                    if os.path.exists(caminho_anexo):
                        self.conn.execute(f"ATTACH DATABASE '{caminho_anexo}' AS {alias}")

            except Exception as e:
                print(f"Erro fatal ao conectar ou anexar bancos SQLite: {e}")
                raise

        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()