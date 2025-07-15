# migrar_dados.py - Migra dados SQLite para PostgreSQL
import sqlite3
import psycopg2
import os
from config import Config

# URL do PostgreSQL (pública do Railway)
POSTGRES_URL = "postgresql://postgres:uzVUcFKomVccdwGGtwrGeyOHWcrjxiIu@ballast.proxy.rlwy.net:11664/railway"

def conectar_postgres():
    """Conecta ao PostgreSQL do Railway"""
    try:
        conn = psycopg2.connect(POSTGRES_URL)
        print("✅ Conectado ao PostgreSQL!")
        return conn
    except Exception as e:
        print(f"❌ Erro PostgreSQL: {e}")
        return None

def conectar_sqlite(banco_tipo):
    """Conecta ao SQLite local"""
    bancos = {
        'saldos': 'dados/db/banco_saldo_receita.db',
        'lancamentos': 'dados/db/banco_lancamento_receita.db', 
        'dimensoes': 'dados/db/banco_dimensoes.db'
    }
    
    caminho = bancos.get(banco_tipo)
    if not os.path.exists(caminho):
        print(f"❌ Banco {banco_tipo} não encontrado: {caminho}")
        return None
    
    conn = sqlite3.connect(caminho)
    print(f"✅ Conectado ao SQLite ({banco_tipo})!")
    return conn

def criar_tabelas_postgres():
    """Cria as tabelas no PostgreSQL"""
    pg_conn = conectar_postgres()
    if not pg_conn:
        return False
    
    cursor = pg_conn.cursor()
    
    # Tabelas do banco SALDOS
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS fato_saldos (
        id SERIAL PRIMARY KEY,
        COEXERCICIO INTEGER,
        INMES INTEGER,
        CDORGAO TEXT,
        DSORGAO TEXT,
        CDUNIDADE TEXT,
        DSUNIDADE TEXT,
        VLSALDOANTERIOR DECIMAL,
        VLORCAMENTO DECIMAL,
        VLSALDOATUAL DECIMAL
    );
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS dim_tempo (
        id SERIAL PRIMARY KEY,
        COEXERCICIO INTEGER,
        INMES INTEGER,
        NOME_MES TEXT,
        fonte TEXT DEFAULT 'saldos'
    );
    """)
    
    # Tabelas do banco LANÇAMENTOS
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS lancamentos (
        id SERIAL PRIMARY KEY,
        COEXERCICIO INTEGER,
        INMES INTEGER,
        CDORGAO TEXT,
        DSORGAO TEXT,
        CDUNIDADE TEXT,
        DSUNIDADE TEXT,
        VLLANC DECIMAL,
        DTLANC DATE,
        DSHISTORICO TEXT
    );
    """)
    
    # Tabelas do banco DIMENSÕES (principais)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS categorias (
        id SERIAL PRIMARY KEY,
        codigo TEXT,
        descricao TEXT
    );
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS origens (
        id SERIAL PRIMARY KEY,
        codigo TEXT,
        descricao TEXT
    );
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS unidades_gestoras (
        id SERIAL PRIMARY KEY,
        codigo TEXT,
        descricao TEXT
    );
    """)
    
    pg_conn.commit()
    print("✅ Tabelas criadas no PostgreSQL!")
    pg_conn.close()
    return True

def migrar_tabela(banco_tipo, nome_tabela):
    """Migra uma tabela específica"""
    print(f"\n📦 Migrando {nome_tabela} do banco {banco_tipo}...")
    
    # Conecta nos dois bancos
    sqlite_conn = conectar_sqlite(banco_tipo)
    pg_conn = conectar_postgres()
    
    if not sqlite_conn or not pg_conn:
        return False
    
    try:
        # Busca dados do SQLite
        sqlite_cursor = sqlite_conn.cursor()
        sqlite_cursor.execute(f"SELECT * FROM {nome_tabela}")
        dados = sqlite_cursor.fetchall()
        
        # Pega nomes das colunas
        colunas = [desc[0] for desc in sqlite_cursor.description]
        print(f"📊 Encontrados {len(dados)} registros")
        print(f"📋 Colunas: {colunas}")
        
        if not dados:
            print("⚠️  Tabela vazia, pulando...")
            return True
        
        # Prepara insert no PostgreSQL
        pg_cursor = pg_conn.cursor()
        
        # Limpa tabela existente
        pg_cursor.execute(f"DELETE FROM {nome_tabela}")
        
        # Monta query de insert
        placeholders = ','.join(['%s'] * len(colunas))
        colunas_str = ','.join(colunas)
        
        if nome_tabela == 'dim_tempo' and banco_tipo != 'saldos':
            # Para dim_tempo de outros bancos, adiciona fonte
            query = f"INSERT INTO {nome_tabela} ({colunas_str}, fonte) VALUES ({placeholders}, %s)"
            dados_com_fonte = [tuple(list(row) + [banco_tipo]) for row in dados]
            pg_cursor.executemany(query, dados_com_fonte)
        else:
            query = f"INSERT INTO {nome_tabela} ({colunas_str}) VALUES ({placeholders})"
            pg_cursor.executemany(query, dados)
        
        pg_conn.commit()
        print(f"✅ {nome_tabela} migrada com sucesso!")
        
    except Exception as e:
        print(f"❌ Erro na migração de {nome_tabela}: {e}")
        pg_conn.rollback()
        return False
    finally:
        sqlite_conn.close()
        pg_conn.close()
    
    return True

def migrar_tudo():
    """Migra todos os dados"""
    print("🚀 INICIANDO MIGRAÇÃO COMPLETA")
    print("=" * 50)
    
    # 1. Criar tabelas
    if not criar_tabelas_postgres():
        print("❌ Falha ao criar tabelas")
        return
    
    # 2. Migrar dados prioritários (banco saldos)
    print("\n📦 MIGRANDO BANCO SALDOS (prioritário)")
    migrar_tabela('saldos', 'fato_saldos')
    migrar_tabela('saldos', 'dim_tempo')
    
    # 3. Migrar lançamentos
    print("\n📦 MIGRANDO BANCO LANÇAMENTOS")
    migrar_tabela('lancamentos', 'lancamentos')
    
    # 4. Migrar dimensões principais
    print("\n📦 MIGRANDO BANCO DIMENSÕES (principais)")
    migrar_tabela('dimensoes', 'categorias')
    migrar_tabela('dimensoes', 'origens') 
    migrar_tabela('dimensoes', 'unidades_gestoras')
    
    print("\n🎉 MIGRAÇÃO CONCLUÍDA!")
    print("=" * 50)

if __name__ == "__main__":
    migrar_tudo()
    input("Pressione Enter para fechar...")