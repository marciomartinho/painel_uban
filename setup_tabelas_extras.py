# setup_tabelas_extras.py
"""Setup das tabelas extras para o PostgreSQL"""

import os
import psycopg2

def criar_tabelas_extras():
    """Cria as tabelas que o código está tentando usar"""
    
    try:
        # Conecta ao PostgreSQL
        database_url = os.environ.get('DATABASE_URL')
        if not database_url:
            print("❌ DATABASE_URL não encontrada")
            return False
            
        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()
        
        print("🔗 Conectado ao PostgreSQL...")
        
        # Cria schema dimensoes se não existir
        cursor.execute("CREATE SCHEMA IF NOT EXISTS dimensoes")
        print("✅ Schema 'dimensoes' criado")
        
        # Cria tabela unidades_gestoras
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dimensoes.unidades_gestoras (
                COUG VARCHAR(10) PRIMARY KEY,
                NOUG VARCHAR(100) NOT NULL
            )
        """)
        print("✅ Tabela 'dimensoes.unidades_gestoras' criada")
        
        # Insere dados de exemplo
        cursor.execute("""
            INSERT INTO dimensoes.unidades_gestoras (COUG, NOUG) 
            VALUES 
                ('001', 'Secretaria de Administração'),
                ('002', 'Secretaria de Finanças'),
                ('003', 'Secretaria de Educação'),
                ('004', 'Secretaria de Saúde'),
                ('005', 'Secretaria de Obras')
            ON CONFLICT (COUG) DO NOTHING
        """)
        
        # Cria outras tabelas dimensões que podem ser necessárias
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dimensoes.categorias (
                COCATEGORIARECEITA VARCHAR(10) PRIMARY KEY,
                NOCATEGORIARECEITA VARCHAR(100) NOT NULL
            )
        """)
        
        cursor.execute("""
            INSERT INTO dimensoes.categorias (COCATEGORIARECEITA, NOCATEGORIARECEITA) 
            VALUES 
                ('1000', 'Receitas Correntes'),
                ('2000', 'Receitas de Capital'),
                ('3000', 'Receitas Intraorçamentárias'),
                ('9000', 'Deduções das Receitas')
            ON CONFLICT (COCATEGORIARECEITA) DO NOTHING
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dimensoes.origens (
                COFONTERECEITA VARCHAR(10) PRIMARY KEY,
                NOFONTERECEITA VARCHAR(100) NOT NULL
            )
        """)
        
        cursor.execute("""
            INSERT INTO dimensoes.origens (COFONTERECEITA, NOFONTERECEITA) 
            VALUES 
                ('100', 'Receitas Próprias'),
                ('200', 'Transferências Federais'),
                ('300', 'Transferências Estaduais'),
                ('400', 'Outras Receitas')
            ON CONFLICT (COFONTERECEITA) DO NOTHING
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dimensoes.especies (
                COSUBFONTERECEITA VARCHAR(10) PRIMARY KEY,
                NOSUBFONTERECEITA VARCHAR(100) NOT NULL
            )
        """)
        
        cursor.execute("""
            INSERT INTO dimensoes.especies (COSUBFONTERECEITA, NOSUBFONTERECEITA) 
            VALUES 
                ('01', 'Recursos Ordinários'),
                ('02', 'Recursos Vinculados'),
                ('03', 'Recursos de Convênios'),
                ('04', 'Outros Recursos')
            ON CONFLICT (COSUBFONTERECEITA) DO NOTHING
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dimensoes.alineas (
                COALINEA VARCHAR(10) PRIMARY KEY,
                NOALINEA VARCHAR(100) NOT NULL
            )
        """)
        
        cursor.execute("""
            INSERT INTO dimensoes.alineas (COALINEA, NOALINEA) 
            VALUES 
                ('01', 'Tributos'),
                ('02', 'Contribuições'),
                ('03', 'Receita Patrimonial'),
                ('04', 'Receita de Serviços'),
                ('05', 'Transferências Correntes'),
                ('06', 'Outras Receitas Correntes')
            ON CONFLICT (COALINEA) DO NOTHING
        """)
        
        # Commit das alterações
        conn.commit()
        
        # Verifica se as tabelas foram criadas
        cursor.execute("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'dimensoes'
        """)
        
        tabelas = cursor.fetchall()
        print(f"✅ Tabelas criadas no schema 'dimensoes': {len(tabelas)}")
        for tabela in tabelas:
            print(f"   - {tabela[0]}")
        
        cursor.close()
        conn.close()
        
        print("🎉 Setup das tabelas extras concluído!")
        return True
        
    except Exception as e:
        print(f"❌ Erro ao criar tabelas extras: {e}")
        return False

if __name__ == "__main__":
    criar_tabelas_extras()