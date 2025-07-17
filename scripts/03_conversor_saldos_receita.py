import pandas as pd
import sqlite3
import os
import time

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

def calcular_saldo_contabil(conta_contabil, debito, credito):
    """Calcula o saldo contábil."""
    conta_str = str(conta_contabil).strip()
    debito = float(debito) if debito else 0.0
    credito = float(credito) if credito else 0.0
    if conta_str.startswith('5'): return debito - credito
    if conta_str.startswith('6'): return credito - debito
    return 0.0

def extrair_campos_cocontacorrente(df):
    """Extrai os campos do cocontacorrente."""
    print("  - Extraindo campos do cocontacorrente...")
    df['cocontacorrente'] = df['cocontacorrente'].astype(str).str.strip()
    df['categoriareceita'] = df['cocontacorrente'].str[0:1]
    df['cofontereceita'] = df['cocontacorrente'].str[0:2]
    df['cosubfontereceita'] = df['cocontacorrente'].str[0:3]
    df['corubrica'] = df['cocontacorrente'].str[0:4]
    df['coalinea'] = df['cocontacorrente'].str[0:6]
    df['cofonte'] = df['cocontacorrente'].str[8:17]
    return df

def processar_saldos():
    """Processa o arquivo de saldos e cria o banco de dados"""
    print("=" * 60)
    print("CONVERSOR DE SALDOS DE RECEITA (PADRONIZADO)")
    print("=" * 60)
    
    start_time = time.time()
    
    arquivo_excel = os.path.join(CAMINHO_DADOS_BRUTOS, 'ReceitaSaldo.xlsx')
    caminho_db = os.path.join(CAMINHO_DB, 'banco_saldo_receita.db')
    
    if not os.path.exists(arquivo_excel):
        print(f"\n❌ ERRO: Arquivo '{arquivo_excel}' não encontrado!")
        return
    
    if os.path.exists(caminho_db):
        resposta = input("\nBanco de saldos já existe. Deseja substituí-lo? (s/n): ")
        if resposta.lower() != 's':
            print("Operação cancelada.")
            return
        os.remove(caminho_db)
        print("Banco antigo removido.")
    
    print("\n--- Processando Saldos ---")
    
    try:
        print("  - Lendo arquivo Excel...")
        df = pd.read_excel(arquivo_excel, dtype=str)
        
        df.columns = [col.lower() for col in df.columns]
        print("  - Nomes de colunas convertidos para minúsculas.")

        print(f"  - Total de registros: {len(df):,}")
        
        df = extrair_campos_cocontacorrente(df)
        
        print("\n  - Convertendo colunas numéricas...")
        df['vadebito'] = pd.to_numeric(df['vadebito'], errors='coerce').fillna(0)
        df['vacredito'] = pd.to_numeric(df['vacredito'], errors='coerce').fillna(0)
        
        print("\n  - Calculando saldo contábil...")
        df['saldo_contabil'] = df.apply(lambda row: calcular_saldo_contabil(row['cocontacontabil'], row['vadebito'], row['vacredito']), axis=1)
        
        conn = sqlite3.connect(caminho_db)
        cursor = conn.cursor()

        print("\n  - Salvando no banco de dados...")
        df.to_sql('fato_saldos', conn, if_exists='replace', index=False, chunksize=10000)
        
        # <<< CORREÇÃO: Adicionando a criação da tabela dim_tempo de volta >>>
        print("\n  - Criando tabela de períodos (dim_tempo)...")
        cursor.execute("""
        CREATE TABLE dim_tempo AS
        SELECT DISTINCT 
            coexercicio,
            inmes,
            CASE inmes
                WHEN 1 THEN 'Janeiro' WHEN 2 THEN 'Fevereiro' WHEN 3 THEN 'Março'
                WHEN 4 THEN 'Abril' WHEN 5 THEN 'Maio' WHEN 6 THEN 'Junho'
                WHEN 7 THEN 'Julho' WHEN 8 THEN 'Agosto' WHEN 9 THEN 'Setembro'
                WHEN 10 THEN 'Outubro' WHEN 11 THEN 'Novembro' WHEN 12 THEN 'Dezembro'
            END as nome_mes
        FROM fato_saldos
        ORDER BY coexercicio, inmes
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_tempo_periodo ON dim_tempo (coexercicio, inmes)")
        print("    ✅ Tabela 'dim_tempo' criada com sucesso.")
        # <<< FIM DA CORREÇÃO >>>

        print("\n  - Criando índices...")
        indices = ["coalinea", "cofonte", "cocontacontabil", "coug", "coexercicio", "inmes", "saldo_contabil"]
        for col in indices:
            cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_saldo_{col} ON fato_saldos ({col})")
        
        conn.commit()
        conn.close()
        
        end_time = time.time()
        print(f"\n✅ Processamento concluído em {end_time - start_time:.2f} segundos!")
        
    except Exception as e:
        print(f"\n❌ ERRO durante o processamento: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    processar_saldos()