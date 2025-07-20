# povoar_fontes_postgres.py
import os
import pandas as pd
from sqlalchemy import create_engine, text
import time
import chardet

# --- CONFIGURAÇÃO ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CAMINHO_DADOS_BRUTOS = os.path.join(BASE_DIR, 'dados', 'dados_brutos', 'dimensao')

def detectar_encoding(arquivo_path):
    """Detecta automaticamente o encoding do arquivo"""
    with open(arquivo_path, 'rb') as file:
        raw_data = file.read(100000)
        result = chardet.detect(raw_data)
        return result['encoding']

def ler_csv_otimizado(arquivo_path):
    """Lê CSV com detecção automática de encoding e separador"""
    encoding = detectar_encoding(arquivo_path)
    
    # Tenta diferentes separadores
    for sep in [';', ',']:
        try:
            df = pd.read_csv(arquivo_path, 
                           encoding=encoding, 
                           sep=sep, 
                           on_bad_lines='skip',
                           low_memory=False,
                           dtype=str)
            if len(df.columns) > 1:
                print(f"   📝 Encoding: {encoding}, Separador: '{sep}'")
                return df
        except:
            continue
    
    raise ValueError(f"Não foi possível ler {arquivo_path}")

def criar_engine_postgres():
    """Cria engine PostgreSQL"""
    # Primeiro tenta usar DATABASE_URL se disponível
    database_url = os.getenv('DATABASE_URL')
    
    if database_url:
        # Corrige URL se necessário
        if database_url.startswith("postgres://"):
            database_url = database_url.replace("postgres://", "postgresql://", 1)
        connection_string = database_url
        print(f"🔗 Usando DATABASE_URL fornecida")
    else:
        # Fallback para variáveis individuais
        db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432'),
            'database': os.getenv('DB_NAME', 'financas_publicas'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD')  # Sem valor padrão
        }
        
        if not db_config['password']:
            raise ValueError("DB_PASSWORD deve ser definido nas variáveis de ambiente")
        
        connection_string = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        print(f"🔗 Conectando em {db_config['host']}:{db_config['port']}/{db_config['database']}")
    
    engine = create_engine(
        connection_string,
        pool_size=10,
        max_overflow=0,
        pool_pre_ping=True
    )
    
    return engine

def main():
    print("=" * 60)
    print("POVOADOR DE TABELA FONTES - POSTGRESQL")
    print("=" * 60)
    
    # Caminho do arquivo
    arquivo_fontes = os.path.join(CAMINHO_DADOS_BRUTOS, 'fonte.csv')
    
    # Verifica se arquivo existe
    if not os.path.exists(arquivo_fontes):
        print(f"\n❌ Arquivo não encontrado: {arquivo_fontes}")
        print("\nVerifique se o arquivo 'fonte.csv' está em:")
        print(f"  {CAMINHO_DADOS_BRUTOS}")
        return
    
    print(f"\n✅ Arquivo encontrado: fonte.csv")
    
    # Cria engine
    try:
        engine = criar_engine_postgres()
        
        # Testa conexão
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✅ Conexão com PostgreSQL estabelecida!")
        
    except Exception as e:
        print(f"\n❌ Erro ao conectar com PostgreSQL: {e}")
        print("\nVerifique se:")
        print("  1. O PostgreSQL está rodando")
        print("  2. As credenciais estão corretas")
        print("  3. DATABASE_URL está configurada ou DB_* variáveis estão definidas")
        return
    
    # Cria schema se não existir
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS dimensoes"))
        conn.commit()
        print("✅ Schema 'dimensoes' verificado")
    
    # Lê o arquivo CSV
    try:
        print(f"\n📖 Lendo arquivo fonte.csv...")
        df = ler_csv_otimizado(arquivo_fontes)
        
        # Converte nomes de colunas para minúsculas
        df.columns = [col.lower() for col in df.columns]
        print(f"   - {len(df)} registros encontrados")
        print(f"   - Colunas: {', '.join(df.columns)}")
        
        # Remove possíveis duplicatas
        if 'cofonte' in df.columns:
            df = df.drop_duplicates(subset=['cofonte'])
            print(f"   - {len(df)} registros após remover duplicatas")
        
    except Exception as e:
        print(f"\n❌ Erro ao ler arquivo: {e}")
        return
    
    # Carrega no PostgreSQL
    try:
        print(f"\n📤 Carregando dados na tabela dimensoes.fontes...")
        start_time = time.time()
        
        # Carrega dados
        df.to_sql(
            'fontes', 
            engine, 
            schema='dimensoes',
            if_exists='replace', 
            index=False,
            method='multi',
            chunksize=1000
        )
        
        tempo_total = time.time() - start_time
        print(f"✅ Tabela criada com sucesso em {tempo_total:.2f}s")
        
        # Cria índice
        with engine.connect() as conn:
            print(f"\n🔧 Criando índice...")
            try:
                conn.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_fontes_cofonte 
                    ON dimensoes.fontes (cofonte)
                """))
                conn.commit()
                print("✅ Índice criado")
            except Exception as e:
                print(f"⚠️  Erro ao criar índice: {e}")
        
        # Verifica resultado
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) as total 
                FROM dimensoes.fontes
            """))
            total = result.fetchone()[0]
            
            print(f"\n📊 Verificação final:")
            print(f"   - Total de registros na tabela: {total}")
            
            # Mostra alguns exemplos
            result = conn.execute(text("""
                SELECT cofonte, nofonte 
                FROM dimensoes.fontes 
                LIMIT 5
            """))
            
            print(f"\n   Exemplos de dados:")
            for row in result:
                print(f"     - {row.cofonte}: {row.nofonte}")
        
        print(f"\n✅ Processo concluído com sucesso!")
        print(f"   A tabela 'dimensoes.fontes' foi criada e povoada.")
        
    except Exception as e:
        print(f"\n❌ Erro ao carregar dados: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()