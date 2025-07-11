import pandas as pd
import sqlite3
import os
import time
from datetime import datetime

# --- 1. CONFIGURAÇÃO ---
# Detecta o diretório base do projeto
if os.path.basename(os.getcwd()) == 'scripts':
    BASE_DIR = os.path.dirname(os.getcwd())
else:
    BASE_DIR = os.getcwd()

print(f"Diretório base do projeto: {BASE_DIR}")

# Configuração da tabela de saldos
TABELA_SALDOS = {
    'nome_tabela': 'fato_saldos',
    'arquivo_excel': 'ReceitaSaldo.xlsx',
    'caminho': os.path.join(BASE_DIR, 'dados_brutos')
}

# --- 2. FUNÇÕES DE TRANSFORMAÇÃO ---

def transformar_dados_saldo(df):
    """
    Aplica transformações necessárias no DataFrame de saldos.
    Inclui a extração de substrings do COCONTACORRENTE.
    """
    print("  - Aplicando transformações nos dados...")
    
    # Garante que COCONTACORRENTE seja string e remove espaços
    if 'COCONTACORRENTE' in df.columns:
        df['COCONTACORRENTE'] = df['COCONTACORRENTE'].astype(str).str.strip()
        
        # Mostra amostra para debug
        if len(df) > 0:
            print(f"    Amostra de COCONTACORRENTE: '{df['COCONTACORRENTE'].iloc[0]}'")
            print(f"    Tamanho: {len(df['COCONTACORRENTE'].iloc[0])}")
        
        # Extrai os substrings (mesma lógica dos lançamentos)
        df['CATEGORIARECEITA'] = df['COCONTACORRENTE'].str[0:1]      # 1º dígito
        df['ORIGEM']           = df['COCONTACORRENTE'].str[0:2]      # 2 primeiros dígitos
        df['ESPECIE']          = df['COCONTACORRENTE'].str[0:3]      # 3 primeiros dígitos
        df['ESPECIFICACAO']    = df['COCONTACORRENTE'].str[0:4]      # 4 primeiros dígitos
        df['ALINEA']           = df['COCONTACORRENTE'].str[0:6]      # 6 primeiros dígitos
        df['FONTE']            = df['COCONTACORRENTE'].str[8:17]     # do 9º ao 17º dígito
        
        print("    ✓ Substrings extraídos de COCONTACORRENTE")
    
    # Garante que COCONTACONTABIL seja string
    if 'COCONTACONTABIL' in df.columns:
        df['COCONTACONTABIL'] = df['COCONTACONTABIL'].astype(str).str.strip()
    
    # Garante que COUG seja string
    if 'COUG' in df.columns:
        df['COUG'] = df['COUG'].astype(str).str.strip()
    
    # Converte valores numéricos
    colunas_numericas = ['VALORSALDO', 'SALDO', 'VALOR', 'VALANCAMENTO']  # Possíveis nomes da coluna de saldo
    for col in colunas_numericas:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            print(f"    ✓ Coluna numérica '{col}' processada")
    
    # Se houver uma coluna de data/período, garantir formato correto
    colunas_data = ['DATAREFERENCIA', 'PERIODO', 'MES', 'DATACOMPETENCIA']
    for col in colunas_data:
        if col in df.columns:
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce')
            except:
                pass  # Mantém como está se não conseguir converter
    
    return df

def criar_indices_saldos(conn):
    """
    Cria índices para otimizar consultas na tabela de saldos.
    """
    print("\n  - Criando índices para otimizar consultas...")
    cursor = conn.cursor()
    
    # Lista de índices a criar (incluindo os novos campos de substring)
    indices = [
        'CREATE INDEX idx_saldo_conta ON fato_saldos (COCONTACONTABIL)',
        'CREATE INDEX idx_saldo_ug ON fato_saldos (COUG)',
        'CREATE INDEX idx_saldo_mes ON fato_saldos (INMES)',
        'CREATE INDEX idx_saldo_exercicio ON fato_saldos (COEXERCICIO)',
        'CREATE INDEX idx_saldo_conta_mes ON fato_saldos (COCONTACONTABIL, INMES)',
        'CREATE INDEX idx_saldo_ug_conta_mes ON fato_saldos (COUG, COCONTACONTABIL, INMES)',
        'CREATE INDEX idx_saldo_categoria ON fato_saldos (CATEGORIARECEITA)',
        'CREATE INDEX idx_saldo_origem ON fato_saldos (ORIGEM)',
        'CREATE INDEX idx_saldo_especie ON fato_saldos (ESPECIE)',
        'CREATE INDEX idx_saldo_especificacao ON fato_saldos (ESPECIFICACAO)',
        'CREATE INDEX idx_saldo_alinea ON fato_saldos (ALINEA)',
        'CREATE INDEX idx_saldo_fonte ON fato_saldos (FONTE)'
    ]
    
    for idx_sql in indices:
        try:
            cursor.execute(idx_sql)
            print(f"    ✓ Índice criado: {idx_sql.split('ON')[0].strip()}")
        except sqlite3.OperationalError as e:
            if "already exists" in str(e):
                print(f"    - Índice já existe: {idx_sql.split('ON')[0].strip()}")
            else:
                print(f"    ! Erro ao criar índice: {e}")
    
    conn.commit()

# --- 3. FUNÇÃO PRINCIPAL ---

def processar_saldos_receita():
    """
    Processa o arquivo de saldos e adiciona ao banco de dados existente.
    """
    start_time = time.time()
    
    # Caminho do banco de dados existente
    caminho_db = os.path.join(BASE_DIR, 'banco_lancamento_receita.db')
    
    if not os.path.exists(caminho_db):
        print(f"❌ ERRO: Banco de dados '{caminho_db}' não encontrado!")
        print("Execute primeiro o conversor de lançamentos para criar o banco base.")
        return
    
    # Caminho do arquivo Excel
    caminho_excel = os.path.join(TABELA_SALDOS['caminho'], TABELA_SALDOS['arquivo_excel'])
    
    print(f"\n--- Processando Saldos de Receita ---")
    print(f"Arquivo: {caminho_excel}")
    
    if not os.path.exists(caminho_excel):
        print(f"❌ ERRO: Arquivo '{caminho_excel}' não encontrado!")
        return
    
    try:
        # Conecta ao banco existente
        conn = sqlite3.connect(caminho_db)
        
        # Lê o arquivo Excel
        print("  - Lendo arquivo Excel...")
        df_saldos = pd.read_excel(caminho_excel)
        
        print(f"  - Total de registros encontrados: {len(df_saldos)}")
        print(f"  - Colunas encontradas: {list(df_saldos.columns)}")
        
        # Mostra amostra dos dados
        print("\n  - Amostra dos dados (primeiras 3 linhas):")
        print(df_saldos.head(3).to_string())
        
        # Aplica transformações
        df_saldos = transformar_dados_saldo(df_saldos)
        
        # Remove a tabela se já existir
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS fato_saldos")
        conn.commit()
        
        # Carrega dados no banco
        print("\n  - Carregando dados no banco...")
        df_saldos.to_sql(TABELA_SALDOS['nome_tabela'], conn, if_exists='replace', index=False)
        
        # Cria índices
        criar_indices_saldos(conn)
        
        # Verifica o resultado
        cursor.execute("SELECT COUNT(*) FROM fato_saldos")
        total = cursor.fetchone()[0]
        print(f"\n✅ Total de registros inseridos na tabela fato_saldos: {total}")
        
        # Mostra estrutura da tabela criada
        cursor.execute("PRAGMA table_info(fato_saldos)")
        colunas = cursor.fetchall()
        print("\n  - Estrutura da tabela fato_saldos:")
        for col in colunas:
            print(f"    • {col[1]} ({col[2]})")
        
        # Exemplo de consulta para verificar
        print("\n  - Exemplo de dados (5 primeiros registros):")
        query_exemplo = "SELECT * FROM fato_saldos LIMIT 5"
        df_exemplo = pd.read_sql_query(query_exemplo, conn)
        print(df_exemplo.to_string())
        
        conn.close()
        
        end_time = time.time()
        print(f"\n✅ Processamento concluído em {end_time - start_time:.2f} segundos!")
        
        # Sugestões de queries úteis
        print("\n📊 QUERIES ÚTEIS PARA ANÁLISE:")
        print("-" * 50)
        print("""
1. Saldo total por mês:
   SELECT INMES, SUM(VALORSALDO) as SALDO_TOTAL 
   FROM fato_saldos 
   GROUP BY INMES 
   ORDER BY INMES;

2. Saldo por categoria de receita:
   SELECT 
       s.CATEGORIARECEITA,
       c.NOCATEGORIARECEITA,
       SUM(s.VALORSALDO) as SALDO_TOTAL
   FROM fato_saldos s
   LEFT JOIN categorias c ON s.CATEGORIARECEITA = c.COCATEGORIARECEITA
   GROUP BY s.CATEGORIARECEITA, c.NOCATEGORIARECEITA;

3. Saldo por origem e espécie:
   SELECT 
       s.ORIGEM,
       o.NOFONTERECEITA as NOME_ORIGEM,
       s.ESPECIE,
       e.NOSUBFONTERECEITA as NOME_ESPECIE,
       SUM(s.VALORSALDO) as SALDO
   FROM fato_saldos s
   LEFT JOIN origens o ON s.ORIGEM = o.COFONTERECEITA
   LEFT JOIN especies e ON s.ESPECIE = e.COSUBFONTERECEITA
   GROUP BY s.ORIGEM, o.NOFONTERECEITA, s.ESPECIE, e.NOSUBFONTERECEITA;

4. Comparação saldos vs lançamentos por categoria:
   SELECT 
       s.CATEGORIARECEITA,
       s.INMES,
       SUM(s.VALORSALDO) as SALDO,
       COALESCE(SUM(l.VALANCAMENTO), 0) as LANCAMENTOS
   FROM fato_saldos s
   LEFT JOIN lancamentos l ON 
       s.CATEGORIARECEITA = l.CATEGORIARECEITA 
       AND s.INMES = l.INMES
   GROUP BY s.CATEGORIARECEITA, s.INMES;
        """)
        
    except Exception as e:
        print(f"\n❌ ERRO durante o processamento: {e}")
        import traceback
        traceback.print_exc()

# --- 4. FUNÇÃO PARA CRIAR DIMENSÃO TEMPO (OPCIONAL) ---

def criar_dimensao_tempo(conn):
    """
    Cria uma tabela de dimensão tempo para facilitar análises temporais.
    """
    print("\n  - Criando dimensão tempo...")
    
    # Query para extrair períodos únicos dos lançamentos e saldos
    query_periodos = """
    SELECT DISTINCT COEXERCICIO, INMES 
    FROM (
        SELECT COEXERCICIO, INMES FROM lancamentos
        UNION
        SELECT COEXERCICIO, INMES FROM fato_saldos
    )
    ORDER BY COEXERCICIO, INMES
    """
    
    df_periodos = pd.read_sql_query(query_periodos, conn)
    
    # Cria descrições dos meses
    meses = {
        1: 'Janeiro', 2: 'Fevereiro', 3: 'Março', 4: 'Abril',
        5: 'Maio', 6: 'Junho', 7: 'Julho', 8: 'Agosto',
        9: 'Setembro', 10: 'Outubro', 11: 'Novembro', 12: 'Dezembro'
    }
    
    df_periodos['NOMES'] = df_periodos['INMES'].map(meses)
    df_periodos['PERIODO'] = df_periodos['COEXERCICIO'].astype(str) + '-' + df_periodos['INMES'].astype(str).str.zfill(2)
    
    # Cria a tabela
    df_periodos.to_sql('dim_tempo', conn, if_exists='replace', index=False)
    
    # Cria índice
    cursor = conn.cursor()
    cursor.execute('CREATE INDEX idx_tempo_periodo ON dim_tempo (COEXERCICIO, INMES)')
    conn.commit()
    
    print(f"    ✓ Dimensão tempo criada com {len(df_periodos)} períodos")

# --- 5. EXECUÇÃO ---

if __name__ == '__main__':
    print("=" * 60)
    print("CONVERSOR DE SALDOS DE RECEITA")
    print("=" * 60)
    
    processar_saldos_receita()
    
    # Pergunta se deseja criar dimensão tempo
    resposta = input("\n🕐 Deseja criar a dimensão tempo? (s/n): ")
    if resposta.lower() == 's':
        conn = sqlite3.connect(os.path.join(BASE_DIR, 'banco_lancamento_receita.db'))
        criar_dimensao_tempo(conn)
        conn.close()
        print("✅ Dimensão tempo criada com sucesso!")