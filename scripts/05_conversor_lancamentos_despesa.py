# scripts/05_conversor_lancamentos_despesa.py
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
    'COEXERCICIO', 'COUG', 'COGESTAO', 'NUDOCUMENTO', 'COEVENTO', 'COCONTACONTABIL',
    'COCONTACORRENTE', 'INMES', 'DALANCAMENTO', 'VALANCAMENTO', 'INDEBITOCREDITO',
    'INABREENCERRA', 'COUGDESTINO', 'COGESTAODESTINO', 'DATRANSACAO', 'COUGCONTAB', 'COGESTAOCONTAB'
]

def extrair_campos_despesa(row):
    """Extrai todos os campos orçamentários a partir do cocontacorrente."""
    s = str(row['cocontacorrente']).strip()
    
    # Lista de todos os campos que podem ser criados
    campos_orcamentarios = ['inesfera', 'couo', 'cofuncao', 'cosubfuncao', 'coprograma', 'coprojeto', 
                          'cosubtitulo', 'cofonte', 'conatureza', 'incategoria', 'cogrupo', 
                          'comodalidade', 'coelemento', 'subelemento', 'coclasseorc']
    
    # Inicializa todos os campos como None (nulo)
    for campo in campos_orcamentarios:
        row[campo] = None

    if len(s) == 38 or len(s) == 40:
        row['inesfera'] = s[0]
        row['couo'] = s[1:6]
        row['cofuncao'] = s[6:8]
        row['cosubfuncao'] = s[8:11]
        row['coprograma'] = s[11:15]
        row['coprojeto'] = s[15:19]
        row['cosubtitulo'] = s[19:23]
        row['cofonte'] = s[23:32]
        row['conatureza'] = s[32:38]
        row['incategoria'] = s[32]
        row['cogrupo'] = s[33]
        row['comodalidade'] = s[34:36]
        row['coelemento'] = s[36:38]
        
        if len(s) == 40:
            row['subelemento'] = s[38:40]
            row['coclasseorc'] = s[32:40]
            
    return row

def processar_lancamentos_despesa():
    print("=" * 60)
    print("CONVERSOR DE LANÇAMENTOS DE DESPESA")
    print("=" * 60)
    
    start_time = time.time()
    
    arquivo_excel = os.path.join(CAMINHO_DADOS_BRUTOS, 'DespesaLancamento.xlsx')
    caminho_db = os.path.join(CAMINHO_DB, 'banco_lancamento_despesa.db')
    
    if not os.path.exists(arquivo_excel):
        print(f"\n❌ ERRO: Arquivo '{arquivo_excel}' não encontrado!")
        return
        
    if os.path.exists(caminho_db):
        resposta = input("\nBanco de lançamentos de despesa já existe. Deseja substituí-lo? (s/n): ")
        if resposta.lower() != 's':
            print("Operação cancelada.")
            return
        os.remove(caminho_db)
        print("Banco antigo removido.")

    try:
        print(f"  - Lendo arquivo Excel (apenas colunas necessárias)...")
        # Lê todas as colunas como texto para evitar problemas de tipo
        df = pd.read_excel(arquivo_excel, usecols=lambda c: c in COLUNAS_NECESSARIAS, dtype=str)
        
        df.columns = [col.lower() for col in df.columns]
        print("  - Nomes de colunas convertidos para minúsculas.")
        print(f"  - Total de registros lidos: {len(df):,}")

        print("  - Extraindo campos orçamentários...")
        df = df.apply(extrair_campos_despesa, axis=1)
        print("    ✅ Novas colunas criadas.")

        conn = sqlite3.connect(caminho_db)
        print("\n  - Salvando no banco de dados...")
        df.to_sql('fato_lancamento_despesa', conn, if_exists='replace', index=False, chunksize=10000)
        
        print("\n  - Criando índices...")
        cursor = conn.cursor()
        indices = ["coexercicio", "coug", "cocontacontabil", "cofonte", "conatureza", "coevento"]
        for col in indices:
             if col in df.columns:
                cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_lanc_despesa_{col} ON fato_lancamento_despesa ({col})")

        conn.commit()
        conn.close()
        
        end_time = time.time()
        print(f"\n✅ Processamento concluído em {end_time - start_time:.2f} segundos!")

    except Exception as e:
        print(f"\n❌ ERRO durante o processamento: {e}")

if __name__ == "__main__":
    processar_lancamentos_despesa()