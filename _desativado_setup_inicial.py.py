# setup_inicial.py - Script para criar estrutura inicial no Railway
import os
import psycopg2

def criar_tabelas_basicas():
    """Cria tabelas básicas no PostgreSQL do Railway"""
    
    # URL do banco (vai vir das variáveis de ambiente no Railway)
    database_url = os.environ.get('DATABASE_URL')
    
    if not database_url:
        print("❌ DATABASE_URL não encontrada nas variáveis de ambiente")
        return False
    
    try:
        print("🔗 Conectando ao PostgreSQL...")
        # Adicionado para compatibilidade com SQLAlchemy
        if database_url.startswith("postgres://"):
            database_url = database_url.replace("postgres://", "postgresql://", 1)
        
        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()
        
        print("🏗️  Criando tabelas básicas...")
        
        # Tabela de tempo
        # <<< CORREÇÃO: Adicionada a coluna 'fonte' >>>
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_tempo (
            id SERIAL PRIMARY KEY,
            coexercicio INTEGER,
            inmes INTEGER,
            nome_mes TEXT,
            fonte TEXT DEFAULT 'saldos',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        
        # Tabela de saldos
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS fato_saldos (
            id SERIAL PRIMARY KEY,
            coexercicio INTEGER,
            inmes INTEGER,
            cdorgao TEXT,
            dsorgao TEXT,
            cdunidade TEXT,
            dsunidade TEXT,
            vlsaldoanterior DECIMAL,
            vlorcamento DECIMAL,
            vlsaldoatual DECIMAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        # Inserir dados de exemplo básicos
        cursor.execute("""
        INSERT INTO dim_tempo (coexercicio, inmes, nome_mes, fonte) 
        VALUES 
            (2024, 6, 'Junho', 'exemplo'),
            (2025, 6, 'Junho', 'exemplo')
        ON CONFLICT DO NOTHING;
        """)
        
        cursor.execute("""
        INSERT INTO fato_saldos (coexercicio, inmes, cdorgao, dsorgao, cdunidade, dsunidade, vlsaldoanterior, vlorcamento, vlsaldoatual) 
        VALUES 
            (2025, 6, '0101', 'Câmara Municipal', '010101', 'Câmara Municipal - Principal', 1000000, 1200000, 1100000),
            (2025, 6, '0201', 'Prefeitura Municipal', '020101', 'Prefeitura Municipal - Principal', 5000000, 6000000, 5500000)
        ON CONFLICT DO NOTHING;
        """)
        
        conn.commit()
        
        # Testar se funcionou
        cursor.execute("SELECT COUNT(*) FROM fato_saldos")
        count_saldos = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM dim_tempo")
        count_tempo = cursor.fetchone()[0]
        
        print(f"✅ Tabelas criadas com sucesso!")
        print(f"📊 fato_saldos: {count_saldos} registros")
        print(f"📊 dim_tempo: {count_tempo} registros")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Erro: {e}")
        return False

if __name__ == "__main__":
    print("🚀 CONFIGURAÇÃO INICIAL DO BANCO DE DADOS")
    print("=" * 50)
    
    if criar_tabelas_basicas():
        print("\n🎉 CONFIGURAÇÃO CONCLUÍDA!")
        print("✅ Banco de dados pronto para uso")
    else:
        print("\n❌ FALHA NA CONFIGURAÇÃO")
        
    # No Railway, não precisa do input
    if not os.environ.get('RAILWAY_ENVIRONMENT'):
        input("Pressione Enter para fechar...")