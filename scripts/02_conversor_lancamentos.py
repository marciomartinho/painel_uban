import pandas as pd
import sqlite3
import os
import time
import sys

# --- CONFIGURAÇÃO ---
if os.path.basename(os.getcwd()) == 'scripts':
    BASE_DIR = os.path.dirname(os.getcwd())
else:
    BASE_DIR = os.getcwd()

# Caminhos
CAMINHO_DADOS_BRUTOS = os.path.join(BASE_DIR, 'dados', 'dados_brutos')
CAMINHO_DB = os.path.join(BASE_DIR, 'dados', 'db')

# Criar pasta db se não existir
os.makedirs(CAMINHO_DB, exist_ok=True)

# Colunas necessárias do Excel (incluindo COCONTACORRENTE para extração)
COLUNAS_EXCEL = [
    'COEXERCICIO',
    'COUG',
    'NUDOCUMENTO',
    'COEVENTO',
    'COCONTACONTABIL',
    'INMES',
    'VALANCAMENTO',
    'INDEBITOCREDITO',
    'COUGCONTAB',
    'COCONTACORRENTE'  # Será usada apenas para extração, não será salva
]

def extrair_campos_cocontacorrente(df):
    """
    Extrai os campos do COCONTACORRENTE e remove a coluna original
    """
    print("  - Extraindo campos do COCONTACORRENTE...")
    
    # Garante que COCONTACORRENTE seja string
    df['COCONTACORRENTE'] = df['COCONTACORRENTE'].astype(str).str.strip()
    
    # Mostra exemplo
    if len(df) > 0:
        exemplo = df['COCONTACORRENTE'].iloc[0]
        print(f"    Exemplo COCONTACORRENTE: '{exemplo}' (tamanho: {len(exemplo)})")
    
    # Extrai os campos
    df['CATEGORIARECEITA'] = df['COCONTACORRENTE'].str[0:1]      # 1º dígito
    df['COFONTERECEITA'] = df['COCONTACORRENTE'].str[0:2]        # 2 primeiros
    df['COSUBFONTERECEITA'] = df['COCONTACORRENTE'].str[0:3]     # 3 primeiros
    df['CORUBRICA'] = df['COCONTACORRENTE'].str[0:4]             # 4 primeiros
    df['COALINEA'] = df['COCONTACORRENTE'].str[0:6]              # 6 primeiros
    df['COFONTE'] = df['COCONTACORRENTE'].str[8:17]              # 9º ao 17º
    
    # Remove a coluna COCONTACORRENTE original
    df = df.drop('COCONTACORRENTE', axis=1)
    
    print("    ✅ Campos extraídos e coluna original removida!")
    
    # Mostra exemplo dos campos extraídos
    if len(df) > 0:
        print(f"    Exemplo de extração:")
        print(f"      CATEGORIARECEITA: {df['CATEGORIARECEITA'].iloc[0]}")
        print(f"      COFONTERECEITA: {df['COFONTERECEITA'].iloc[0]}")
        print(f"      COSUBFONTERECEITA: {df['COSUBFONTERECEITA'].iloc[0]}")
        print(f"      CORUBRICA: {df['CORUBRICA'].iloc[0]}")
        print(f"      COALINEA: {df['COALINEA'].iloc[0]}")
        print(f"      COFONTE: {df['COFONTE'].iloc[0]}")
    
    return df

def otimizar_tipos_dados(df):
    """
    Otimiza os tipos de dados para reduzir o tamanho do banco
    """
    print("\n  - Otimizando tipos de dados...")
    
    memoria_antes = df.memory_usage(deep=True).sum() / 1024**2
    
    # Otimiza tipos numéricos
    if 'COEXERCICIO' in df.columns:
        df['COEXERCICIO'] = df['COEXERCICIO'].astype('int16')
    
    if 'INMES' in df.columns:
        df['INMES'] = df['INMES'].astype('int8')
    
    if 'VALANCAMENTO' in df.columns:
        df['VALANCAMENTO'] = pd.to_numeric(df['VALANCAMENTO'], errors='coerce').fillna(0).astype('float32')
    
    # Converte campos de texto com poucos valores únicos para categoria
    campos_categoria = ['COUG', 'COEVENTO', 'COCONTACONTABIL', 'INDEBITOCREDITO', 'COUGCONTAB',
                       'CATEGORIARECEITA', 'COFONTERECEITA', 'COSUBFONTERECEITA', 
                       'CORUBRICA', 'COALINEA', 'COFONTE']
    
    for campo in campos_categoria:
        if campo in df.columns:
            # Verifica se vale a pena converter para categoria
            num_unique = df[campo].nunique()
            if num_unique < len(df) * 0.5:  # Menos de 50% de valores únicos
                df[campo] = df[campo].astype('category')
                print(f"    → {campo} convertido para categoria ({num_unique} valores únicos)")
    
    memoria_depois = df.memory_usage(deep=True).sum() / 1024**2
    reducao = (1 - memoria_depois/memoria_antes) * 100
    
    print(f"    ✅ Memória reduzida de {memoria_antes:.2f} MB para {memoria_depois:.2f} MB ({reducao:.1f}% de redução)")
    
    return df

def processar_lancamentos():
    """Processa o arquivo de lançamentos e cria o banco de dados otimizado"""
    
    print("=" * 60)
    print("CONVERSOR OTIMIZADO DE LANÇAMENTOS DE RECEITA")
    print("=" * 60)
    
    start_time = time.time()
    
    # Arquivos
    arquivo_excel = os.path.join(CAMINHO_DADOS_BRUTOS, 'ReceitaLancamento.xlsx')
    caminho_db = os.path.join(CAMINHO_DB, 'banco_lancamento_receita.db')
    
    print(f"\nArquivo fonte: {arquivo_excel}")
    print(f"Banco destino: {caminho_db}")
    
    # Verifica se o arquivo existe
    if not os.path.exists(arquivo_excel):
        print(f"\n❌ ERRO: Arquivo '{arquivo_excel}' não encontrado!")
        return
    
    # Verifica se o banco já existe
    if os.path.exists(caminho_db):
        resposta = input("\nBanco de lançamentos já existe. Deseja substituí-lo? (s/n): ")
        if resposta.lower() != 's':
            print("Operação cancelada.")
            return
        os.remove(caminho_db)
        print("Banco antigo removido.")
    
    print("\n--- Processando Lançamentos ---")
    
    try:
        # Lê apenas as colunas necessárias do Excel
        print("  - Lendo arquivo Excel (apenas colunas necessárias)...")
        
        # Primeiro verifica quais colunas existem no arquivo
        df_test = pd.read_excel(arquivo_excel, nrows=5)
        colunas_disponiveis = [col for col in COLUNAS_EXCEL if col in df_test.columns]
        colunas_faltando = [col for col in COLUNAS_EXCEL if col not in df_test.columns]
        
        if colunas_faltando:
            print(f"    ⚠️  Colunas não encontradas: {', '.join(colunas_faltando)}")
        
        # Lê o arquivo completo com as colunas disponíveis
        df = pd.read_excel(
            arquivo_excel,
            usecols=colunas_disponiveis,
            dtype={
                'COCONTACORRENTE': str,
                'COCONTACONTABIL': str,
                'COUG': str,
                'NUDOCUMENTO': str,
                'COEVENTO': str,
                'COUGCONTAB': str,
                'INDEBITOCREDITO': str
            }
        )
        
        print(f"  - Total de registros: {len(df):,}")
        print(f"  - Colunas carregadas: {len(colunas_disponiveis)} de {len(COLUNAS_EXCEL)}")
        
        # Verifica se COCONTACORRENTE está presente
        if 'COCONTACORRENTE' not in df.columns:
            print("\n❌ ERRO: Coluna COCONTACORRENTE não encontrada!")
            return
        
        # Extrai campos do COCONTACORRENTE e remove a coluna original
        df = extrair_campos_cocontacorrente(df)
        
        # Otimiza tipos de dados
        df = otimizar_tipos_dados(df)
        
        # Mostra as colunas finais
        print(f"\n  - Colunas finais no banco: {len(df.columns)}")
        print(f"    {', '.join(sorted(df.columns))}")
        
        # Conecta ao banco com otimizações
        conn = sqlite3.connect(caminho_db)
        cursor = conn.cursor()
        
        # Configurações para otimizar o SQLite
        cursor.execute("PRAGMA journal_mode = WAL")
        cursor.execute("PRAGMA synchronous = NORMAL")
        cursor.execute("PRAGMA cache_size = -64000")  # 64MB de cache
        cursor.execute("PRAGMA temp_store = MEMORY")
        
        # Salva no banco
        print("\n  - Salvando no banco de dados...")
        df.to_sql('lancamentos', conn, if_exists='replace', index=False, chunksize=10000)
        
        # Cria índices estratégicos
        print("\n  - Criando índices...")
        
        indices = [
            # Índices nos campos extraídos
            ("idx_categoriareceita", "CATEGORIARECEITA"),
            ("idx_cofontereceita", "COFONTERECEITA"),
            ("idx_cosubfontereceita", "COSUBFONTERECEITA"),
            ("idx_corubrica", "CORUBRICA"),
            ("idx_coalinea", "COALINEA"),
            ("idx_cofonte", "COFONTE"),
            # Índices nas colunas principais
            ("idx_coug", "COUG"),
            ("idx_cocontacontabil", "COCONTACONTABIL"),
            ("idx_exercicio", "COEXERCICIO"),
            ("idx_mes", "INMES"),
            # Índice composto para consultas temporais
            ("idx_exercicio_mes", "COEXERCICIO, INMES"),
            # Índice composto para análises por UG e período
            ("idx_ug_periodo", "COUG, COEXERCICIO, INMES")
        ]
        
        for nome_idx, campos in indices:
            try:
                cursor.execute(f"CREATE INDEX {nome_idx} ON lancamentos ({campos})")
                print(f"    ✅ {nome_idx}")
            except Exception as e:
                print(f"    ⚠️  {nome_idx}: {e}")
        
        # Cria tabela de períodos
        print("\n  - Criando tabela de períodos...")
        cursor.execute("""
        CREATE TABLE dim_tempo AS
        SELECT DISTINCT 
            COEXERCICIO,
            INMES,
            COEXERCICIO || '-' || printf('%02d', INMES) as PERIODO,
            CASE INMES
                WHEN 1 THEN 'Janeiro'
                WHEN 2 THEN 'Fevereiro'
                WHEN 3 THEN 'Março'
                WHEN 4 THEN 'Abril'
                WHEN 5 THEN 'Maio'
                WHEN 6 THEN 'Junho'
                WHEN 7 THEN 'Julho'
                WHEN 8 THEN 'Agosto'
                WHEN 9 THEN 'Setembro'
                WHEN 10 THEN 'Outubro'
                WHEN 11 THEN 'Novembro'
                WHEN 12 THEN 'Dezembro'
            END as NOME_MES
        FROM lancamentos
        ORDER BY COEXERCICIO, INMES
        """)
        
        cursor.execute("CREATE INDEX idx_tempo_periodo ON dim_tempo (COEXERCICIO, INMES)")
        
        # Executa VACUUM para otimizar o tamanho
        print("\n  - Otimizando o banco (VACUUM)...")
        cursor.execute("VACUUM")
        
        conn.commit()
        
        # Estatísticas
        print("\n--- Estatísticas ---")
        
        # Tamanho do banco
        tamanho_db = os.path.getsize(caminho_db) / 1024**2
        print(f"\n📊 Tamanho do banco: {tamanho_db:.2f} MB")
        
        # Total por ano
        cursor.execute("""
        SELECT COEXERCICIO, COUNT(*) as total, SUM(VALANCAMENTO) as valor_total
        FROM lancamentos
        GROUP BY COEXERCICIO
        ORDER BY COEXERCICIO
        """)
        
        print("\n📈 Resumo por exercício:")
        for row in cursor.fetchall():
            valor = row[2] if row[2] else 0
            print(f"   {row[0]}: {row[1]:,} lançamentos | R$ {valor:,.2f}")
        
        # Exemplo de uso dos campos extraídos
        cursor.execute("""
        SELECT CATEGORIARECEITA, COUNT(*) as total
        FROM lancamentos
        GROUP BY CATEGORIARECEITA
        ORDER BY CATEGORIARECEITA
        """)
        
        print("\n📋 Distribuição por categoria de receita:")
        for row in cursor.fetchall():
            print(f"   Categoria {row[0]}: {row[1]:,} lançamentos")
        
        # Períodos disponíveis
        cursor.execute("SELECT COUNT(*) FROM dim_tempo")
        total_periodos = cursor.fetchone()[0]
        print(f"\n📅 Períodos disponíveis: {total_periodos}")
        
        conn.close()
        
        end_time = time.time()
        print(f"\n✅ Processamento concluído em {end_time - start_time:.2f} segundos!")
        print(f"💾 Banco criado em: {os.path.abspath(caminho_db)}")
        print(f"   Economia estimada: ~{(151 - tamanho_db):.0f} MB ({((151 - tamanho_db)/151*100):.0f}% menor que versão anterior)")
        
    except Exception as e:
        print(f"\n❌ ERRO durante o processamento: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    processar_lancamentos()