# app/modulos/conexao_hibrida.py
"""
Sistema de conexão híbrido - SQLite local + PostgreSQL produção
Ponto central e ÚNICA fonte de verdade para conexões e queries.
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
    Adapta a query removendo prefixos de schema para o ambiente PostgreSQL.
    Ex: 'dimensoes.categorias' se torna 'categorias'.
    """
    if get_db_environment() == 'postgres':
        return query.replace('dimensoes.', '').replace('lancamentos_db.', '')
    return query

class ConexaoBanco:
    """Gerenciador de contexto para garantir que a conexão seja sempre fechada."""

    def __init__(self):
        self.conn = None
        self.env = get_db_environment()

    def __enter__(self):
        if self.env == 'postgres':
            try:
                database_url = os.environ.get('DATABASE_URL')
                if not database_url:
                    raise ConnectionError("Variável de ambiente DATABASE_URL não encontrada.")
                self.conn = psycopg2.connect(database_url)
                self.conn.cursor_factory = psycopg2.extras.RealDictCursor
            except Exception as e:
                print(f"Erro fatal ao conectar ao PostgreSQL: {e}")
                raise
        else: # sqlite
            try:
                # --- CORREÇÃO APLICADA AQUI ---
                # O caminho agora sobe 3 níveis (a partir de /app/modulos) para chegar na raiz do projeto
                project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
                base_path = os.path.join(project_root, 'dados', 'db')
                caminho_principal = os.path.join(base_path, 'banco_saldo_receita.db')

                if not os.path.exists(caminho_principal):
                    raise FileNotFoundError(f"Banco de dados principal não encontrado: {caminho_principal}")

                self.conn = sqlite3.connect(caminho_principal)
                self.conn.row_factory = sqlite3.Row

                bancos_anexar = {
                    'dimensoes': 'banco_dimensoes.db',
                    'lancamentos_db': 'banco_lancamento_receita.db'
                }
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