# scripts/04_conversor_saldos_despesa.py
import pandas as pd
import sqlite3
import os
import time

# --- CONFIGURAÇÃO ---
if os.path.basename(os.getcwd()) == 'scripts':
    BASE_DIR = os.path.dirname(os.getcwd())
else:
    BASE_DIR = os.getcwd()

CAMINHO_DADOS_BRUTOS = os.path.join(BASE_DIR, 'dados', 'dados_brutos')
CAMINHO_DB = os.path.join(BASE_DIR, 'dados', 'db')
os.makedirs(CAMINHO_DB, exist_ok=True)

# Colunas a serem lidas do Excel
COLUNAS_NECESSARIAS = [
    'COEXERCICIO', 'COUG', 'COGESTAO', 'COCONTACONTABIL', 'COCONTACORRENTE', 'INMES',
    'INESFERA', 'COUO', 'COFUNCAO', 'COSUBFUNCAO', 'COPROGRAMA', 'COPROJETO',
    'COSUBTITULO', 'COFONTE', 'CONATUREZA', 'INCATEGORIA', 'VACREDITO', 'VADEBITO', 'INTIPOADM'
]

def extrair_classe_orcamentaria(cocontacorrente):
    """Extrai COCLASSEORC se a conta corrente tiver 40 caracteres."""
    s = str(cocontacorrente).strip()
    if len(s) == 40:
        return s[32:40] # Caracteres 33 a 40 (índice 32 até o 40)
    return None

def processar_saldos_despesa():
    """Processa o arquivo de saldos de despesa e cria o banco de dados."""
    print("=" * 60)
    print("CONVERSOR DE SALDOS DE DESPESA")
    print("=" * 60)
    
    start_time = time.time()
    
    arquivo_excel = os.path.join(CAMINHO_DADOS_BRUTOS, 'DespesaSaldo.xlsx')
    caminho_db = os.path.join(CAMINHO_DB, 'banco_saldo_despesa.db')
    
    if not os.path.exists(arquivo_excel):
        print(f"\n❌ ERRO: Arquivo '{arquivo_excel}' não encontrado!")
        return
    
    if os.path.exists(caminho_db):
        resposta = input("\nBanco de saldos de despesa já existe. Deseja substituí-lo? (s/n): ")
        if resposta.lower() != 's':
            print("Operação cancelada.")
            return
        os.remove(caminho_db)
        print("Banco antigo removido.")

    try:
        print(f"  - Lendo arquivo Excel (apenas colunas necessárias)...")
        df = pd.read_excel(arquivo_excel, usecols=lambda c: c in COLUNAS_NECESSARIAS)
        
        df.columns = [col.lower() for col in df.columns]
        print("  - Nomes de colunas convertidos para minúsculas.")
        print(f"  - Total de registros lidos: {len(df):,}")

        print("  - Extraindo COCLASSEORC...")
        df['coclasseorc'] = df['cocontacorrente'].apply(extrair_classe_orcamentaria)
        print("    ✅ Coluna 'coclasseorc' criada.")

        conn = sqlite3.connect(caminho_db)
        print("\n  - Salvando no banco de dados...")
        df.to_sql('fato_saldo_despesa', conn, if_exists='replace', index=False, chunksize=10000)
        
        print("\n  - Criando índices...")
        cursor = conn.cursor()
        indices = ["coexercicio", "coug", "cocontacontabil", "cofonte", "conatureza"]
        for col in indices:
            if col in df.columns:
                cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_saldo_despesa_{col} ON fato_saldo_despesa ({col})")

        conn.commit()
        conn.close()
        
        end_time = time.time()
        print(f"\n✅ Processamento concluído em {end_time - start_time:.2f} segundos!")

    except Exception as e:
        print(f"\n❌ ERRO durante o processamento: {e}")

if __name__ == "__main__":
    processar_saldos_despesa()