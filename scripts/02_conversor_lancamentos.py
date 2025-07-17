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

os.makedirs(CAMINHO_DB, exist_ok=True)

# Colunas necessárias do Excel
COLUNAS_EXCEL = [
    'COEXERCICIO', 'COUG', 'NUDOCUMENTO', 'COEVENTO', 'COCONTACONTABIL',
    'INMES', 'VALANCAMENTO', 'INDEBITOCREDITO', 'COUGCONTAB', 'COCONTACORRENTE'
]

def extrair_campos_cocontacorrente(df):
    """Extrai os campos do cocontacorrente e remove a coluna original"""
    print("  - Extraindo campos do cocontacorrente...")
    df['cocontacorrente'] = df['cocontacorrente'].astype(str).str.strip()
    df['categoriareceita'] = df['cocontacorrente'].str[0:1]
    df['cofontereceita'] = df['cocontacorrente'].str[0:2]
    df['cosubfontereceita'] = df['cocontacorrente'].str[0:3]
    df['corubrica'] = df['cocontacorrente'].str[0:4]
    df['coalinea'] = df['cocontacorrente'].str[0:6]
    df['cofonte'] = df['cocontacorrente'].str[8:17]
    df = df.drop('cocontacorrente', axis=1)
    print("    ✅ Campos extraídos e coluna original removida!")
    return df

def processar_lancamentos():
    """Processa o arquivo de lançamentos e cria o banco de dados otimizado"""
    print("=" * 60)
    print("CONVERSOR OTIMIZADO DE LANÇAMENTOS DE RECEITA")
    print("=" * 60)
    
    start_time = time.time()
    
    arquivo_excel = os.path.join(CAMINHO_DADOS_BRUTOS, 'ReceitaLancamento.xlsx')
    caminho_db = os.path.join(CAMINHO_DB, 'banco_lancamento_receita.db')
    
    if not os.path.exists(arquivo_excel):
        print(f"\n❌ ERRO: Arquivo '{arquivo_excel}' não encontrado!")
        return
    
    if os.path.exists(caminho_db):
        resposta = input("\nBanco de lançamentos já existe. Deseja substituí-lo? (s/n): ")
        if resposta.lower() != 's':
            print("Operação cancelada.")
            return
        os.remove(caminho_db)
        print("Banco antigo removido.")
    
    print("\n--- Processando Lançamentos ---")
    
    try:
        print("  - Lendo arquivo Excel...")
        df_test = pd.read_excel(arquivo_excel, nrows=5)
        colunas_disponiveis = [col for col in COLUNAS_EXCEL if col in df_test.columns]
        
        # Lê o arquivo sem forçar o tipo de todas as colunas
        df = pd.read_excel(arquivo_excel, usecols=colunas_disponiveis)
        
        df.columns = [col.lower() for col in df.columns]
        print("  - Nomes de colunas convertidos para minúsculas.")
        
        # <<< CORREÇÃO: Tratar os tipos de dados após a leitura >>>
        print("  - Ajustando tipos de dados...")
        for col in df.columns:
            if col == 'valancamento':
                # Converte VALANCAMENTO para número, tratando erros
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                print(f"    - '{col}' convertida para numérico.")
            else:
                # Converte as outras para texto para garantir consistência
                df[col] = df[col].astype(str)
                print(f"    - '{col}' convertida para texto.")

        print(f"  - Total de registros: {len(df):,}")
        
        if 'cocontacorrente' not in df.columns:
            print("\n❌ ERRO: Coluna 'cocontacorrente' não encontrada no arquivo Excel!")
            return
        
        df = extrair_campos_cocontacorrente(df)
        
        conn = sqlite3.connect(caminho_db)
        
        print("\n  - Salvando no banco de dados...")
        df.to_sql('lancamentos', conn, if_exists='replace', index=False, chunksize=10000)
        
        print("\n  - Criando índices...")
        cursor = conn.cursor()
        indices = ["coalinea", "cofonte", "cocontacontabil", "coug", "coexercicio", "inmes"]
        for col in indices:
            cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_lancamento_{col} ON lancamentos ({col})")
        
        conn.commit()
        conn.close()
        
        end_time = time.time()
        print(f"\n✅ Processamento concluído em {end_time - start_time:.2f} segundos!")
        
    except Exception as e:
        print(f"\n❌ ERRO durante o processamento: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    processar_lancamentos()