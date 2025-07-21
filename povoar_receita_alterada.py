# povoar_receita_alterada.py
# Este script é uma versão focada para carregar apenas as tabelas de receita
# que tiveram a lógica de transformação do 'cocontacorrente' alterada.
# Versão compatível com Pandas < 1.2.0

import os
import pandas as pd
from sqlalchemy import create_engine, text
import time

# --- CONFIGURAÇÃO ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CAMINHO_DADOS_BRUTOS = os.path.join(BASE_DIR, 'dados', 'dados_brutos')

CHUNK_SIZE = 50000

ARQUIVOS_PARA_POVOAR = {
    'ReceitaSaldo.xlsx': ('public', 'fato_saldos'),
    'ReceitaLancamento.xlsx': ('public', 'lancamentos')
}

INDICES_CONFIG = {
    'fato_saldos': [
        ('idx_saldo_periodo', 'coexercicio, inmes'),
        ('idx_saldo_alinea', 'coalinea'),
        ('idx_saldo_fonte', 'cofonte'),
        ('idx_saldo_conta', 'cocontacontabil'),
        ('idx_saldo_ug', 'coug'),
        ('idx_saldo_valor', 'saldo_contabil')
    ],
    'lancamentos': [
        ('idx_lanc_periodo', 'coexercicio, inmes'),
        ('idx_lanc_alinea', 'coalinea'),
        ('idx_lanc_fonte', 'cofonte'),
        ('idx_lanc_conta', 'cocontacontabil'),
        ('idx_lanc_ug', 'coug'),
        ('idx_lanc_valor', 'valancamento')
    ]
}

def criar_engine_postgres():
    """Cria engine de conexão com o PostgreSQL usando variáveis de ambiente."""
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
    
    return create_engine(connection_string, pool_pre_ping=True)

def processar_valor_monetario_vetorizado(serie):
    """Processa valores monetários em formato string para float."""
    mask_str = serie.apply(lambda x: isinstance(x, str))
    if not mask_str.any():
        return pd.to_numeric(serie, errors='coerce').fillna(0).astype('float32')

    valores_str = serie[mask_str].astype(str).str.strip()
    valores_str = valores_str.str.replace('R$', '', regex=False).str.replace('$', '', regex=False).str.strip()
    
    mask_br = valores_str.str.contains(',') & ~valores_str.str.contains(r'\.')
    mask_us = ~mask_br
    
    valores_str.loc[mask_br] = valores_str.loc[mask_br].str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
    valores_str.loc[mask_us] = valores_str.loc[mask_us].str.replace(',', '', regex=False)
    
    serie.loc[mask_str] = pd.to_numeric(valores_str, errors='coerce')
    return pd.to_numeric(serie, errors='coerce').fillna(0).astype('float32')

def aplicar_transformacoes_receita(df):
    """Aplica a nova regra de transformação para cocontacorrente e calcula saldos."""
    if 'cocontacorrente' in df.columns:
        cc = df['cocontacorrente'].astype(str).str.strip()
        colunas_novas = ['categoriareceita', 'cofontereceita', 'cosubfontereceita', 'corubrica', 'coalinea',
                         'inesfera', 'couo', 'cofuncao', 'cosubfuncao', 'coprograma', 'coprojeto', 
                         'cosubtitulo', 'conatureza', 'incategoria', 'cogrupo', 'comodalidade', 'coelemento', 'cofonte']
        for col in colunas_novas:
            df[col] = pd.NA

        mask_17 = cc.str.len() == 17
        mask_38 = cc.str.len() == 38

        if mask_17.any():
            df.loc[mask_17, 'categoriareceita'] = cc[mask_17].str[0:1]
            df.loc[mask_17, 'cofontereceita'] = cc[mask_17].str[0:2]
            df.loc[mask_17, 'cosubfontereceita'] = cc[mask_17].str[0:3]
            df.loc[mask_17, 'corubrica'] = cc[mask_17].str[0:4]
            df.loc[mask_17, 'coalinea'] = cc[mask_17].str[0:6]
            df.loc[mask_17, 'cofonte'] = cc[mask_17].str[8:17]

        if mask_38.any():
            df.loc[mask_38, 'inesfera'] = cc[mask_38].str[0:1]
            df.loc[mask_38, 'couo'] = cc[mask_38].str[1:6]
            df.loc[mask_38, 'cofuncao'] = cc[mask_38].str[6:8]
            df.loc[mask_38, 'cosubfuncao'] = cc[mask_38].str[8:11]
            df.loc[mask_38, 'coprograma'] = cc[mask_38].str[11:15]
            df.loc[mask_38, 'coprojeto'] = cc[mask_38].str[15:19]
            df.loc[mask_38, 'cosubtitulo'] = cc[mask_38].str[19:23]
            df.loc[mask_38, 'cofonte'] = cc[mask_38].str[23:32]
            df.loc[mask_38, 'conatureza'] = cc[mask_38].str[32:38]
            df.loc[mask_38, 'incategoria'] = cc[mask_38].str[32:33]
            df.loc[mask_38, 'cogrupo'] = cc[mask_38].str[33:34]
            df.loc[mask_38, 'comodalidade'] = cc[mask_38].str[34:36]
            df.loc[mask_38, 'coelemento'] = cc[mask_38].str[36:38]
            
    if all(c in df.columns for c in ['cocontacontabil', 'vadebito', 'vacredito']):
        conta_str = df['cocontacontabil'].astype(str).str.strip()
        mask_5 = conta_str.str.startswith('5')
        mask_6 = conta_str.str.startswith('6')
        
        df['saldo_contabil'] = 0.0
        df.loc[mask_5, 'saldo_contabil'] = df.loc[mask_5, 'vadebito'] - df.loc[mask_5, 'vacredito']
        df.loc[mask_6, 'saldo_contabil'] = df.loc[mask_6, 'vacredito'] - df.loc[mask_6, 'vadebito']
        df['saldo_contabil'] = df['saldo_contabil'].astype('float32')

    return df

def processar_chunk_dados(chunk):
    """Prepara um chunk para inserção no banco."""
    chunk.columns = [col.lower() for col in chunk.columns]
    
    colunas_monetarias = ['valancamento', 'vadebito', 'vacredito']
    for col in colunas_monetarias:
        if col in chunk.columns:
            chunk[col] = processar_valor_monetario_vetorizado(chunk[col])
    
    chunk = aplicar_transformacoes_receita(chunk)
    
    return chunk

# LÓGICA DE LEITURA ALTERNATIVA - INÍCIO
def ler_excel_compativel_chunks(arquivo_path, chunksize):
    """
    Função de leitura de Excel em chunks compatível com versões antigas do Pandas.
    """
    print("    - Lendo cabeçalho do Excel...")
    df_header = pd.read_excel(arquivo_path, nrows=0)
    
    print("    - Contando total de registros do Excel...")
    # Lê apenas a primeira coluna para contar as linhas rapidamente
    df_count = pd.read_excel(arquivo_path, usecols=[0])
    total_rows = len(df_count)
    print(f"    - Total de registros: {total_rows:,}")
    del df_count
    
    # Itera sobre o arquivo lendo em pedaços
    for start_row in range(0, total_rows, chunksize):
        chunk = pd.read_excel(
            arquivo_path,
            header=None,  # O cabeçalho é ignorado nas leituras de chunk
            skiprows=start_row + 1, # Pula o cabeçalho original + linhas já lidas
            nrows=chunksize
        )
        chunk.columns = df_header.columns # Aplica o cabeçalho lido no início
        yield chunk
# LÓGICA DE LEITURA ALTERNATIVA - FIM

def criar_indices(engine, schema, tabela, indices):
    """Cria os índices para uma tabela."""
    with engine.connect() as conn:
        for idx_name, idx_cols in indices:
            try:
                print(f"    - Criando índice {idx_name}...")
                sql = f"CREATE INDEX IF NOT EXISTS {idx_name} ON {schema}.{tabela} ({idx_cols})"
                conn.execute(text(sql))
                conn.commit()
            except Exception as e:
                print(f"      ⚠️  Erro ao criar índice {idx_name}: {e}")
                conn.rollback()

def main():
    """Função principal para executar a carga dos dados."""
    print("=" * 60)
    print("POVOADOR FOCADO - TABELAS DE RECEITA ALTERADAS")
    print("=" * 60)
    
    engine = criar_engine_postgres()
    
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("\n✅ Conexão com PostgreSQL estabelecida!")
    except Exception as e:
        print(f"\n❌ Erro de conexão com PostgreSQL: {e}")
        return

    print("\n🚀 Iniciando carga dos dados de receita...\n")
    
    for arquivo, (schema, tabela) in ARQUIVOS_PARA_POVOAR.items():
        arquivo_path = os.path.join(CAMINHO_DADOS_BRUTOS, arquivo)
        
        if not os.path.exists(arquivo_path):
            print(f"⚠️ Arquivo não encontrado, pulando: {arquivo}")
            continue

        start_time = time.time()
        total_registros = 0
        print(f"📁 Processando {arquivo} -> {schema}.{tabela}")

        try:
            primeiro_chunk = True
            
            # USA A NOVA FUNÇÃO DE LEITURA COMPATÍVEL
            leitor_chunks = ler_excel_compativel_chunks(arquivo_path, CHUNK_SIZE)

            for i, chunk in enumerate(leitor_chunks):
                print(f"    - Processando chunk {i+1}...")
                chunk_processado = processar_chunk_dados(chunk)
                
                if 'cocontacorrente' in chunk_processado.columns:
                    chunk_processado = chunk_processado.drop('cocontacorrente', axis=1)

                if primeiro_chunk:
                    chunk_processado.to_sql(
                        tabela, engine, schema=schema,
                        if_exists='replace', index=False,
                        method='multi', chunksize=10000
                    )
                    primeiro_chunk = False
                else:
                    chunk_processado.to_sql(
                        tabela, engine, schema=schema,
                        if_exists='append', index=False,
                        method='multi', chunksize=10000
                    )
                
                total_registros += len(chunk)
                print(f"    ✓ Chunk {i+1}: {len(chunk):,} registros (Total: {total_registros:,})")
            
            tempo_total = time.time() - start_time
            print(f"  ✅ {total_registros:,} registros carregados em {tempo_total:.1f}s")

            if tabela in INDICES_CONFIG:
                print(f"  🔧 Criando índices para {tabela}...")
                criar_indices(engine, schema, tabela, INDICES_CONFIG[tabela])

        except Exception as e:
            print(f"  ❌ Erro ao processar o arquivo {arquivo}: {e}")

    print("\n✅ Processo concluído!")

if __name__ == "__main__":
    main()