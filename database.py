# database.py - Conexão universal para SQLite e PostgreSQL
import sqlite3
import os
from config import Config

def get_connection(banco_tipo='saldos'):
    """
    Retorna conexão apropriada
    banco_tipo: 'saldos', 'lancamentos', 'dimensoes' (só usado no SQLite local)
    """
    config = Config()
    
    if config.DATABASE_TYPE == 'postgresql':
        try:
            import psycopg2
            print(f"🔗 Conectando ao PostgreSQL...")
            conn = psycopg2.connect(config.DATABASE_URL)
            print("✅ Conexão PostgreSQL estabelecida!")
            return conn
        except Exception as e:
            print(f"❌ Erro ao conectar PostgreSQL: {e}")
            raise
    else:
        try:
            caminho_banco = config.BANCOS.get(banco_tipo)
            if not caminho_banco:
                raise ValueError(f"Tipo de banco '{banco_tipo}' não encontrado")
            
            print(f"🔗 Conectando ao SQLite ({banco_tipo}): {caminho_banco}")
            if not os.path.exists(caminho_banco):
                print(f"⚠️  Banco {banco_tipo} não encontrado: {caminho_banco}")
                return None
            
            conn = sqlite3.connect(caminho_banco)
            print(f"✅ Conexão SQLite ({banco_tipo}) estabelecida!")
            return conn
        except Exception as e:
            print(f"❌ Erro ao conectar SQLite ({banco_tipo}): {e}")
            raise

def execute_query(query, params=None):
    """Executa uma query e retorna os resultados"""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        
        # Se for SELECT, retorna os dados
        if query.strip().upper().startswith('SELECT'):
            return cursor.fetchall()
        else:
            # Se for INSERT/UPDATE/DELETE, faz commit
            conn.commit()
            return cursor.rowcount
            
    except Exception as e:
        print(f"❌ Erro na query: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

def test_connection():
    """Testa a conexão com o banco"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        config = Config()
        if config.DATABASE_TYPE == 'postgresql':
            cursor.execute("SELECT version();")
            version = cursor.fetchone()[0]
            print(f"✅ PostgreSQL conectado: {version}")
        else:
            cursor.execute("SELECT sqlite_version();")
            version = cursor.fetchone()[0]
            print(f"✅ SQLite conectado: versão {version}")
        
        conn.close()
        return True
    except Exception as e:
        print(f"❌ Teste de conexão falhou: {e}")
        return False