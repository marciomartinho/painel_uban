# app/modulos/conexao_hibrida.py
"""Sistema de conexão híbrido - SQLite local + PostgreSQL produção"""

import os
import sqlite3

def get_database_connection(banco_tipo='saldos'):
    """
    Retorna conexão apropriada baseada no ambiente
    banco_tipo: 'saldos', 'lancamentos', 'dimensoes' (usado apenas no SQLite)
    """
    
    # Detecta se está em produção (Railway)
    if os.environ.get('RAILWAY_ENVIRONMENT') or os.environ.get('DATABASE_URL'):
        return get_postgresql_connection()
    else:
        return get_sqlite_connection(banco_tipo)

def get_postgresql_connection():
    """Conexão PostgreSQL para produção"""
    try:
        import psycopg2
        database_url = os.environ.get('DATABASE_URL')
        
        if not database_url:
            raise ValueError("DATABASE_URL não encontrada nas variáveis de ambiente")
        
        conn = psycopg2.connect(database_url)
        return conn
    except ImportError:
        raise ImportError("psycopg2 não está instalado")
    except Exception as e:
        raise ConnectionError(f"Erro ao conectar PostgreSQL: {e}")

def get_sqlite_connection(banco_tipo='saldos'):
    """Conexão SQLite para desenvolvimento local"""
    
    # Mapeia tipos de banco para arquivos
    bancos_map = {
        'saldos': 'banco_saldo_receita.db',
        'lancamentos': 'banco_lancamento_receita.db',
        'dimensoes': 'banco_dimensoes.db'
    }
    
    # Constrói o caminho correto
    base_path = os.path.join(os.path.dirname(__file__), '../../dados/db')
    arquivo_banco = bancos_map.get(banco_tipo, 'banco_saldo_receita.db')
    caminho_completo = os.path.join(base_path, arquivo_banco)
    
    if not os.path.exists(caminho_completo):
        raise FileNotFoundError(f"Banco SQLite não encontrado: {caminho_completo}")
    
    return sqlite3.connect(caminho_completo)

def execute_query(query, params=None, banco_tipo='saldos'):
    """
    Executa query de forma híbrida
    """
    conn = get_database_connection(banco_tipo)
    
    try:
        cursor = conn.cursor()
        
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        
        # Se for SELECT, retorna dados
        if query.strip().upper().startswith('SELECT'):
            return cursor.fetchall()
        else:
            # Se for INSERT/UPDATE/DELETE, faz commit
            conn.commit()
            return cursor.rowcount
    finally:
        conn.close()

def get_database_info():
    """Retorna informações sobre o banco atual"""
    if os.environ.get('RAILWAY_ENVIRONMENT') or os.environ.get('DATABASE_URL'):
        return {
            'tipo': 'PostgreSQL',
            'ambiente': 'Produção (Railway)',
            'url': os.environ.get('DATABASE_URL', 'Não definida')[:50] + '...'
        }
    else:
        return {
            'tipo': 'SQLite',
            'ambiente': 'Desenvolvimento (Local)',
            'caminho': 'dados/db/'
        }

# Classe compatível com código existente
class ConexaoBanco:
    """Classe para manter compatibilidade com código existente"""
    
    @staticmethod
    def conectar_completo():
        """Conecta ao banco principal (saldos) com anexos para SQLite"""
        if os.environ.get('RAILWAY_ENVIRONMENT') or os.environ.get('DATABASE_URL'):
            # Em produção, retorna conexão PostgreSQL simples
            return get_postgresql_connection()
        else:
            # Em desenvolvimento, conecta SQLite com anexos
            conn = get_sqlite_connection('saldos')
            
            # Anexa outros bancos
            try:
                base_path = os.path.join(os.path.dirname(__file__), '../../dados/db')
                
                lancamentos_path = os.path.join(base_path, 'banco_lancamento_receita.db')
                if os.path.exists(lancamentos_path):
                    conn.execute(f"ATTACH DATABASE '{lancamentos_path}' AS lancamentos_db")
                
                dimensoes_path = os.path.join(base_path, 'banco_dimensoes.db')
                if os.path.exists(dimensoes_path):
                    conn.execute(f"ATTACH DATABASE '{dimensoes_path}' AS dimensoes")
                
            except Exception as e:
                print(f"Aviso: Erro ao anexar bancos: {e}")
            
            return conn
    
    @staticmethod
    def fechar_conexao(conn):
        """Fecha conexão"""
        if conn:
            conn.close()