# povoar_dimensoes_despesa.py - Script dedicado para novas dimensões de despesa
import os
import pandas as pd
from sqlalchemy import create_engine, text
import time
import chardet

# --- CONFIGURAÇÃO ---
if os.path.basename(os.getcwd()) == 'scripts':
    BASE_DIR = os.path.dirname(os.getcwd())
else:
    BASE_DIR = os.getcwd()

# Caminho para os dados brutos de dimensão
CAMINHO_DADOS_BRUTOS = os.path.join(BASE_DIR, 'dados', 'dados_brutos', 'dimensao')

# --- ARQUIVOS E ÍNDICES PARA ESTA TAREFA ESPECÍFICA ---

# Apenas os dois novos arquivos de dimensão de despesa
ARQUIVOS_PARA_POVOAR = {
    'despesa_funcao.csv': ('dimensoes', 'funcoes'),
    'despesa_subfuncao.csv': ('dimensoes', 'subfuncoes'),
}

# Configuração de índices apenas para as novas tabelas
INDICES_CONFIG = {
    'funcoes': [('idx_funcoes_pk', 'cofuncao')],
    'subfuncoes': [('idx_subfuncoes_pk', 'cosubfuncao')],
}

# --- FUNÇÕES UTILITÁRIAS (Reaproveitadas do seu script original) ---

def detectar_encoding(arquivo_path):
    """Detecta automaticamente o encoding do arquivo."""
    with open(arquivo_path, 'rb') as file:
        raw_data = file.read(100000)
        result = chardet.detect(raw_data)
        return result['encoding']

def ler_csv_com_encoding_correto(arquivo_csv):
    """Tenta ler o CSV com diferentes encodings e separadores."""
    encodings = ['utf-8', 'utf-8-sig', 'latin1', 'iso-8859-1', 'cp1252']
    separadores = [';', ',']
    
    detected_encoding = detectar_encoding(arquivo_csv)
    if detected_encoding:
        encodings.insert(0, detected_encoding)
    
    for encoding in encodings:
        for sep in separadores:
            try:
                df = pd.read_csv(arquivo_csv, encoding=encoding, on_bad_lines='skip', sep=sep, dtype=str)
                if len(df.columns) > 1:
                    print(f"   - Encoding: {encoding}, Separador: '{sep}'")
                    return df
            except Exception:
                continue
    
    raise ValueError(f"Não foi possível ler ou parsear o arquivo {arquivo_csv}")

def criar_engine_postgres():
    """Cria engine PostgreSQL usando variáveis de ambiente."""
    database_url = os.getenv('DATABASE_URL')
    
    if database_url:
        connection_string = database_url
        print("🔗 Usando DATABASE_URL para conexão.")
    else:
        db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432'),
            'database': os.getenv('DB_NAME', 'financas_publicas'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', 'postgres')
        }
        connection_string = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        print(f"🔗 Conectando em {db_config['host']}:{db_config['port']}/{db_config['database']}")
    
    return create_engine(connection_string)

def criar_indices(engine, schema, tabela, indices):
    """Cria índices para uma tabela."""
    with engine.connect() as conn:
        for idx_name, idx_cols in indices:
            try:
                print(f"    - Criando índice {idx_name} em {idx_cols}...")
                sql = f"CREATE INDEX IF NOT EXISTS {idx_name} ON {schema}.{tabela} ({idx_cols})"
                conn.execute(text(sql))
                conn.commit()
            except Exception as e:
                print(f"      ⚠️  Erro ao criar índice {idx_name}: {e}")
                conn.rollback()

def processar_e_carregar(arquivo_nome, schema, tabela, engine):
    """Processa um único arquivo de dimensão e carrega no PostgreSQL."""
    start_time = time.time()
    arquivo_path = os.path.join(CAMINHO_DADOS_BRUTOS, arquivo_nome)
    
    print(f"\n📁 Processando {arquivo_nome} -> {schema}.{tabela}")

    if not os.path.exists(arquivo_path):
        print("  ❌ Erro: Arquivo não encontrado!")
        return
        
    try:
        # Ler o CSV
        df = ler_csv_com_encoding_correto(arquivo_path)
        
        # Transformação simples: colunas em minúsculo
        df.columns = [col.lower() for col in df.columns]
        
        # Carregar para o PostgreSQL, substituindo a tabela se existir
        df.to_sql(
            tabela, engine, schema=schema,
            if_exists='replace', index=False,
            method='multi', chunksize=10000
        )
        
        total_registros = len(df)
        tempo_total = time.time() - start_time
        print(f"  ✅ Tabela '{tabela}' criada com {total_registros:,} registros em {tempo_total:.2f}s.")
        
        # Criar índices
        if tabela in INDICES_CONFIG:
            criar_indices(engine, schema, tabela, INDICES_CONFIG[tabela])
            
    except Exception as e:
        print(f"  ❌ Erro ao processar o arquivo {arquivo_nome}: {e}")

def main():
    """Função principal para executar o povoamento."""
    print("=" * 60)
    print("POVOADOR DE DIMENSÕES DE DESPESA (Função e Subfunção)")
    print("=" * 60)
    
    engine = criar_engine_postgres()
    
    # Testa a conexão
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("\n✅ Conexão com PostgreSQL estabelecida com sucesso!")
    except Exception as e:
        print(f"\n❌ Falha na conexão com PostgreSQL: {e}")
        return
        
    # Cria o schema 'dimensoes' se ele não existir
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS dimensoes"))
        conn.commit()

    # Processa cada arquivo definido na configuração
    for arquivo, (schema, tabela) in ARQUIVOS_PARA_POVOAR.items():
        processar_e_carregar(arquivo, schema, tabela, engine)

    print("\n🎉 Processo concluído com sucesso!")

if __name__ == "__main__":
    main()