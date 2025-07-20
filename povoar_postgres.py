# povoar_postgres.py - Versão Super Otimizada e Corrigida
import os
import pandas as pd
from sqlalchemy import create_engine, text
import psycopg2
from io import StringIO
import time
import chardet
import numpy as np

# --- CONFIGURAÇÃO ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CAMINHO_DADOS_BRUTOS = os.path.join(BASE_DIR, 'dados', 'dados_brutos')

# Tamanho do chunk para processamento
CHUNK_SIZE = 100000  # 100k registros por vez

ARQUIVOS_PARA_POVOAR = {
    # Dimensões de Receita e Gerais
    'dimensao/receita_categoria.csv': ('dimensoes', 'categorias'),
    'dimensao/receita_origem.csv': ('dimensoes', 'origens'),
    'dimensao/receita_especie.csv': ('dimensoes', 'especies'),
    'dimensao/receita_especificacao.csv': ('dimensoes', 'especificacoes'),
    'dimensao/receita_alinea.csv': ('dimensoes', 'alineas'),
    'dimensao/fonte.xlsx': ('dimensoes', 'fontes'),
    'dimensao/contacontabil.xlsx': ('dimensoes', 'contas'),
    'dimensao/unidadegestora.csv': ('dimensoes', 'unidades_gestoras'),
    'dimensao/elemento.csv': ('dimensoes', 'elemento'),
    'dimensao/gestao.csv': ('dimensoes', 'gestao'),
    
    # Dimensões de Despesa
    'dimensao/despesa_grupo.xlsx': ('dimensoes', 'despesa_grupo'),
    'dimensao/despesa_categoria.xlsx': ('dimensoes', 'despesa_categoria'),
    'dimensao/despesa_modalidade.xlsx': ('dimensoes', 'despesa_modalidade'),
    'dimensao/classificacaoorcamentaria.xlsx': ('dimensoes', 'classificacao_orcamentaria'),
    
    # Tabelas Fato de Receita
    'ReceitaSaldo.xlsx': ('public', 'fato_saldos'),
    'ReceitaLancamento.xlsx': ('public', 'lancamentos'),
    
    # Tabelas Fato de Despesa
    'DespesaSaldo.xlsx': ('public', 'fato_saldo_despesa'),
    'DespesaLancamento.xlsx': ('public', 'fato_lancamento_despesa')
}

# Configuração de índices para cada tabela
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
    ],
    'fato_saldo_despesa': [
        ('idx_saldo_desp_periodo', 'coexercicio, inmes'),
        ('idx_saldo_desp_ug', 'coug'),
        ('idx_saldo_desp_conta', 'cocontacontabil'),
        ('idx_saldo_desp_fonte', 'cofonte'),
        ('idx_saldo_desp_natureza', 'conatureza')
    ],
    'fato_lancamento_despesa': [
        ('idx_lanc_desp_periodo', 'coexercicio, inmes'),
        ('idx_lanc_desp_ug', 'coug'),
        ('idx_lanc_desp_conta', 'cocontacontabil'),
        ('idx_lanc_desp_fonte', 'cofonte'),
        ('idx_lanc_desp_natureza', 'conatureza'),
        ('idx_lanc_desp_evento', 'coevento'),
        ('idx_lanc_desp_valor', 'valancamento')
    ]
}

def detectar_encoding(arquivo_path):
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
                           low_memory=False)
            if len(df.columns) > 1:
                return df
        except:
            continue
    
    raise ValueError(f"Não foi possível ler {arquivo_path}")

def processar_valor_monetario_vetorizado(serie):
    """Processa valores monetários de forma vetorizada"""
    mask_str = serie.apply(lambda x: isinstance(x, str))
    
    if mask_str.any():
        valores_str = serie[mask_str].astype(str).str.strip()
        valores_str = valores_str.str.replace('R$', '', regex=False).str.replace('$', '', regex=False).str.strip()
        
        mask_br = valores_str.str.contains(',') & ~valores_str.str.contains(r'\.')
        mask_us = ~mask_br
        
        valores_str.loc[mask_br] = valores_str.loc[mask_br].str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
        valores_str.loc[mask_us] = valores_str.loc[mask_us].str.replace(',', '', regex=False)
        
        serie.loc[mask_str] = pd.to_numeric(valores_str, errors='coerce')
    
    return pd.to_numeric(serie, errors='coerce').fillna(0).astype('float32')

def extrair_campos_receita(df):
    """Extrai campos específicos de receita"""
    if 'cocontacorrente' in df.columns:
        cc = df['cocontacorrente'].astype(str).str.strip()
        df['categoriareceita'] = cc.str[0:1]
        df['cofontereceita'] = cc.str[0:2]
        df['cosubfontereceita'] = cc.str[0:3]
        df['corubrica'] = cc.str[0:4]
        df['coalinea'] = cc.str[0:6]
        df['cofonte'] = cc.str[8:17]
    
    # Calcula saldo contábil para fato_saldos
    if 'cocontacontabil' in df.columns and 'vadebito' in df.columns and 'vacredito' in df.columns:
        conta_str = df['cocontacontabil'].astype(str).str.strip()
        mask_5 = conta_str.str.startswith('5')
        mask_6 = conta_str.str.startswith('6')
        
        df['saldo_contabil'] = 0
        df.loc[mask_5, 'saldo_contabil'] = df.loc[mask_5, 'vadebito'] - df.loc[mask_5, 'vacredito']
        df.loc[mask_6, 'saldo_contabil'] = df.loc[mask_6, 'vacredito'] - df.loc[mask_6, 'vadebito']
    
    return df

def extrair_campos_despesa(df):
    """Extrai campos específicos de despesa"""
    if 'cocontacorrente' in df.columns:
        cc = df['cocontacorrente'].astype(str).str.strip()
        
        # Máscaras para tamanhos válidos
        mask_38_40 = (cc.str.len() == 38) | (cc.str.len() == 40)
        mask_40 = cc.str.len() == 40
        
        # Inicializa colunas
        campos = ['inesfera', 'couo', 'cofuncao', 'cosubfuncao', 'coprograma', 
                  'coprojeto', 'cosubtitulo', 'cofonte', 'conatureza', 'incategoria', 
                  'cogrupo', 'comodalidade', 'coelemento', 'subelemento', 'coclasseorc']
        
        for campo in campos:
            df[campo] = None
        
        # Extração vetorizada
        df.loc[mask_38_40, 'inesfera'] = cc[mask_38_40].str[0]
        df.loc[mask_38_40, 'couo'] = cc[mask_38_40].str[1:6]
        df.loc[mask_38_40, 'cofuncao'] = cc[mask_38_40].str[6:8]
        df.loc[mask_38_40, 'cosubfuncao'] = cc[mask_38_40].str[8:11]
        df.loc[mask_38_40, 'coprograma'] = cc[mask_38_40].str[11:15]
        df.loc[mask_38_40, 'coprojeto'] = cc[mask_38_40].str[15:19]
        df.loc[mask_38_40, 'cosubtitulo'] = cc[mask_38_40].str[19:23]
        df.loc[mask_38_40, 'cofonte'] = cc[mask_38_40].str[23:32]
        df.loc[mask_38_40, 'conatureza'] = cc[mask_38_40].str[32:38]
        df.loc[mask_38_40, 'incategoria'] = cc[mask_38_40].str[32]
        df.loc[mask_38_40, 'cogrupo'] = cc[mask_38_40].str[33]
        df.loc[mask_38_40, 'comodalidade'] = cc[mask_38_40].str[34:36]
        df.loc[mask_38_40, 'coelemento'] = cc[mask_38_40].str[36:38]
        
        # Campos específicos para 40 caracteres
        df.loc[mask_40, 'subelemento'] = cc[mask_40].str[38:40]
        df.loc[mask_40, 'coclasseorc'] = cc[mask_40].str[32:40]
    
    return df

def processar_chunk_dados(chunk, tabela_nome):
    """Processa um chunk de dados aplicando transformações necessárias"""
    # Nomes em minúsculas
    chunk.columns = [col.lower() for col in chunk.columns]
    
    # Processa valores monetários
    colunas_monetarias = ['valancamento', 'vadebito', 'vacredito']
    for col in colunas_monetarias:
        if col in chunk.columns:
            chunk[col] = processar_valor_monetario_vetorizado(chunk[col])
    
    # Aplica transformações específicas por tipo de tabela
    if tabela_nome in ['fato_saldos', 'lancamentos']:
        chunk = extrair_campos_receita(chunk)
    elif tabela_nome in ['fato_saldo_despesa', 'fato_lancamento_despesa']:
        chunk = extrair_campos_despesa(chunk)
    
    # Converte colunas de texto
    colunas_texto = [col for col in chunk.columns if col not in colunas_monetarias + ['saldo_contabil']]
    for col in colunas_texto:
        if col in chunk.columns:
            chunk[col] = chunk[col].astype(str)
    
    return chunk

def processar_excel_em_chunks(arquivo_excel, chunk_size=100000):
    """
    Lê arquivo Excel em chunks manualmente
    """
    # Lê apenas as primeiras linhas para pegar os headers
    df_header = pd.read_excel(arquivo_excel, nrows=0)
    colunas = df_header.columns.tolist()
    
    # Para arquivos menores (dimensões), lê tudo de uma vez
    file_size = os.path.getsize(arquivo_excel)
    if file_size < 10 * 1024 * 1024:  # Menor que 10MB
        df = pd.read_excel(arquivo_excel)
        yield df
        return
    
    # Para arquivos grandes, conta o número total de linhas
    print("    - Contando total de registros...")
    df_count = pd.read_excel(arquivo_excel, usecols=[0])
    total_rows = len(df_count)
    print(f"    - Total de registros: {total_rows:,}")
    del df_count  # Libera memória
    
    # Processa em chunks
    for start_row in range(0, total_rows, chunk_size):
        end_row = min(start_row + chunk_size, total_rows)
        
        if start_row == 0:
            # Primeira leitura inclui header
            chunk = pd.read_excel(
                arquivo_excel,
                nrows=chunk_size
            )
        else:
            # Leituras subsequentes pulam o header e linhas anteriores
            chunk = pd.read_excel(
                arquivo_excel,
                skiprows=range(1, start_row + 1),
                nrows=chunk_size,
                header=0
            )
        
        yield chunk

def criar_engine_postgres():
    """Cria engine PostgreSQL com configurações otimizadas"""
    # Primeiro tenta usar DATABASE_URL se disponível
    database_url = os.getenv('DATABASE_URL')
    
    if database_url:
        connection_string = database_url
        print(f"🔗 Usando DATABASE_URL fornecida")
    else:
        # Fallback para variáveis individuais
        db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432'),
            'database': os.getenv('DB_NAME', 'financas_publicas'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', 'postgres')
        }
        
        connection_string = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        print(f"🔗 Conectando em {db_config['host']}:{db_config['port']}/{db_config['database']}")
    
    engine = create_engine(
        connection_string,
        pool_size=20,
        max_overflow=0,
        pool_pre_ping=True,
        pool_recycle=3600,
        connect_args={
            'connect_timeout': 10,
            'options': '-c statement_timeout=300000'  # 5 minutos
        }
    )
    
    return engine

def criar_indices(engine, schema, tabela, indices):
    """Cria índices de forma paralela"""
    with engine.connect() as conn:
        # Desabilita autocommit para batch
        conn.execute(text("SET synchronous_commit = OFF"))
        conn.execute(text("SET maintenance_work_mem = '2GB'"))
        
        for idx_name, idx_cols in indices:
            try:
                print(f"    - Criando índice {idx_name}...")
                # Remove CONCURRENTLY pois pode dar erro em transações
                sql = f"CREATE INDEX IF NOT EXISTS {idx_name} ON {schema}.{tabela} ({idx_cols})"
                conn.execute(text(sql))
                conn.commit()
            except Exception as e:
                print(f"      ⚠️  Erro ao criar índice {idx_name}: {e}")
                conn.rollback()

def processar_arquivo(arquivo_path, schema, tabela, engine):
    """Processa um arquivo e carrega no PostgreSQL"""
    start_time = time.time()
    total_registros = 0
    
    print(f"\n📁 Processando {os.path.basename(arquivo_path)} -> {schema}.{tabela}")
    
    # Verifica extensão
    ext = os.path.splitext(arquivo_path)[1].lower()
    
    try:
        if ext == '.csv':
            # Processa CSV em chunks nativamente
            encoding = detectar_encoding(arquivo_path)
            
            # Para CSVs pequenos (dimensões)
            file_size = os.path.getsize(arquivo_path)
            if file_size < 10 * 1024 * 1024:  # Menor que 10MB
                df = ler_csv_otimizado(arquivo_path)
                df_processado = processar_chunk_dados(df, tabela)
                
                # Converte nomes de colunas para minúsculas
                df_processado.columns = [col.lower() for col in df_processado.columns]
                
                # Salva no PostgreSQL
                df_processado.to_sql(
                    tabela, engine, schema=schema,
                    if_exists='replace', index=False,
                    method='multi', chunksize=10000
                )
                total_registros = len(df_processado)
                print(f"    ✓ {total_registros:,} registros processados")
            else:
                # Para CSVs grandes, usa chunks
                df_reader = pd.read_csv(
                    arquivo_path,
                    chunksize=CHUNK_SIZE,
                    encoding=encoding,
                    sep=None,  # Detecção automática
                    engine='python',
                    on_bad_lines='skip'
                )
                
                for i, chunk in enumerate(df_reader):
                    chunk_processado = processar_chunk_dados(chunk, tabela)
                    
                    # Carrega usando COPY para máxima performance
                    with engine.raw_connection() as conn:
                        cur = conn.cursor()
                        output = StringIO()
                        chunk_processado.to_csv(output, sep='\t', header=False, index=False, na_rep='')
                        output.seek(0)
                        
                        # Cria tabela se não existir (primeira iteração)
                        if i == 0:
                            chunk_processado.head(0).to_sql(
                                tabela, engine, schema=schema, 
                                if_exists='replace', index=False
                            )
                        
                        # COPY rápido
                        columns = ','.join(chunk_processado.columns)
                        cur.copy_expert(
                            f"COPY {schema}.{tabela} ({columns}) FROM STDIN WITH CSV DELIMITER E'\\t' NULL ''",
                            output
                        )
                        conn.commit()
                    
                    total_registros += len(chunk)
                    print(f"    ✓ Chunk {i+1}: {len(chunk):,} registros (Total: {total_registros:,})")
                
        else:  # Excel
            # Para Excel, processa em chunks manualmente
            primeiro_chunk = True
            
            for i, chunk in enumerate(processar_excel_em_chunks(arquivo_path, CHUNK_SIZE)):
                chunk_processado = processar_chunk_dados(chunk, tabela)
                
                # Primeira iteração cria a tabela
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
        if tempo_total > 0:
            print(f"  ✅ {total_registros:,} registros em {tempo_total:.1f}s ({total_registros/tempo_total:.0f} registros/s)")
        
        # Cria índices se configurado
        if tabela in INDICES_CONFIG:
            print(f"  🔧 Criando índices para {tabela}...")
            criar_indices(engine, schema, tabela, INDICES_CONFIG[tabela])
            
    except Exception as e:
        print(f"  ❌ Erro: {e}")
        import traceback
        traceback.print_exc()

def main():
    print("=" * 80)
    print("POVOADOR POSTGRESQL - VERSÃO SUPER OTIMIZADA E CORRIGIDA")
    print("=" * 80)
    
    # Cria engine
    engine = criar_engine_postgres()
    
    # Testa conexão
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("\n✅ Conexão com PostgreSQL estabelecida!")
    except Exception as e:
        print(f"\n❌ Erro ao conectar com PostgreSQL: {e}")
        print("\nVerifique se:")
        print("  1. O PostgreSQL está rodando no servidor especificado")
        print("  2. O banco existe no servidor")
        print("  3. As credenciais estão corretas")
        print("  4. O servidor aceita conexões remotas")
        print("\nDica: Use DATABASE_URL ou configure as variáveis:")
        print("  export DATABASE_URL='postgresql://usuario:senha@host:porta/banco'")
        print("  ou")
        print("  export DB_HOST=host DB_PORT=porta DB_NAME=banco DB_USER=usuario DB_PASSWORD=senha")
        return
    
    # Cria schema se não existir
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS dimensoes"))
        conn.commit()
    
    print("\n🚀 Iniciando carga de dados...\n")
    
    # Processa cada arquivo
    for arquivo_relativo, (schema, tabela) in ARQUIVOS_PARA_POVOAR.items():
        arquivo_path = os.path.join(CAMINHO_DADOS_BRUTOS, arquivo_relativo)
        
        if os.path.exists(arquivo_path):
            processar_arquivo(arquivo_path, schema, tabela, engine)
        else:
            print(f"\n⚠️  Arquivo não encontrado: {arquivo_relativo}")
    
    # Otimizações finais
    print("\n🔧 Executando otimizações finais...")
    with engine.connect() as conn:
        # Atualiza estatísticas
        conn.execute(text("ANALYZE"))
        
        # Cria dim_tempo se não existir
        try:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS public.dim_tempo AS
                SELECT DISTINCT 
                    coexercicio::int,
                    inmes::int,
                    CASE inmes::int
                        WHEN 1 THEN 'Janeiro' WHEN 2 THEN 'Fevereiro' WHEN 3 THEN 'Março'
                        WHEN 4 THEN 'Abril' WHEN 5 THEN 'Maio' WHEN 6 THEN 'Junho'
                        WHEN 7 THEN 'Julho' WHEN 8 THEN 'Agosto' WHEN 9 THEN 'Setembro'
                        WHEN 10 THEN 'Outubro' WHEN 11 THEN 'Novembro' WHEN 12 THEN 'Dezembro'
                    END as nome_mes
                FROM public.fato_saldos
                WHERE coexercicio IS NOT NULL AND inmes IS NOT NULL
                ORDER BY coexercicio, inmes
            """))
            conn.commit()
            print("  ✅ Tabela dim_tempo criada")
        except Exception as e:
            print(f"  ⚠️  Erro ao criar dim_tempo: {e}")
    
    print("\n✅ Processo concluído com sucesso!")
    print("\n📊 Resumo das tabelas criadas:")
    
    # Mostra estatísticas
    with engine.connect() as conn:
        for schema in ['public', 'dimensoes']:
            try:
                result = conn.execute(text(f"""
                    SELECT 
                        schemaname,
                        tablename,
                        n_live_tup as registros
                    FROM pg_stat_user_tables
                    WHERE schemaname = '{schema}'
                    ORDER BY tablename
                """))
                
                print(f"\n  Schema: {schema}")
                for row in result:
                    print(f"    - {row.tablename}: {row.registros:,} registros")
            except Exception as e:
                print(f"\n  ⚠️  Erro ao obter estatísticas do schema {schema}: {e}")

if __name__ == "__main__":
    main()