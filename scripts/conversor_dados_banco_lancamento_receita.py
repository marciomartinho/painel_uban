import pandas as pd
import sqlite3
import os
import time
import sys

# --- 1. CONFIGURAÇÃO CENTRALIZADA ---
# Detecta o diretório base do projeto
if os.path.basename(os.getcwd()) == 'scripts':
    BASE_DIR = os.path.dirname(os.getcwd())
else:
    BASE_DIR = os.getcwd()

print(f"Diretório base do projeto: {BASE_DIR}")

# Tabela Fato
TABELA_FATO = {
    'nome_tabela': 'lancamentos',
    'arquivo_excel': 'ReceitaLancamento.xlsx',  # Mudança aqui
    'caminho_relativo': os.path.join(BASE_DIR, 'dados_brutos')
}

# Tabelas Dimensão
TABELAS_DIMENSAO = {
    'categorias': {
        'arquivo_csv': 'receita_categoria.csv',
        'chave_primaria': 'COCATEGORIARECEITA'
    },
    'origens': {
        'arquivo_csv': 'receita_origem.csv',
        'chave_primaria': 'COFONTERECEITA'
    },
    'especies': {
        'arquivo_csv': 'receita_especie.csv',
        'chave_primaria': 'COSUBFONTERECEITA'
    },
    'especificacoes': {
        'arquivo_csv': 'receita_especificacao.csv',
        'chave_primaria': 'CORUBRICA'
    },
    'alineas': {
        'arquivo_csv': 'receita_alinea.csv',
        'chave_primaria': 'COALINEA'
    },
    'fontes': {
        'arquivo_csv': 'fonte.csv',
        'chave_primaria': 'COFONTE'
    },
    'contas': {
        'arquivo_csv': 'contacontabil.csv',
        'chave_primaria': 'COCONTACONTABIL'
    },
    'unidades_gestoras': {
        'arquivo_csv': 'unidadegestora.csv',
        'chave_primaria': 'COUG'
    }
}

# --- 2. FUNÇÕES DE TRANSFORMAÇÃO E CARGA ---

def transformar_chunk_fato(chunk):
    """
    Aplica as transformações no chunk da tabela fato.
    """
    # Garante que COCONTACORRENTE seja string e remove espaços
    chunk['COCONTACORRENTE'] = chunk['COCONTACORRENTE'].astype(str).str.strip()
    
    # Para debug - mostra informações sobre os primeiros registros
    if len(chunk) > 0:
        print(f"    Amostra de COCONTACORRENTE: '{chunk['COCONTACORRENTE'].iloc[0]}'")
        print(f"    Tamanho: {len(chunk['COCONTACORRENTE'].iloc[0])}")
    
    # Cria as novas colunas com base nos substrings
    chunk['CATEGORIARECEITA'] = chunk['COCONTACORRENTE'].str[0:1]      # 1º dígito
    chunk['ORIGEM']           = chunk['COCONTACORRENTE'].str[0:2]      # 2 primeiros dígitos
    chunk['ESPECIE']          = chunk['COCONTACORRENTE'].str[0:3]      # 3 primeiros dígitos
    chunk['ESPECIFICACAO']    = chunk['COCONTACORRENTE'].str[0:4]      # 4 primeiros dígitos
    chunk['ALINEA']           = chunk['COCONTACORRENTE'].str[0:6]      # 6 primeiros dígitos
    chunk['FONTE']            = chunk['COCONTACORRENTE'].str[8:17]     # do 9º ao 17º dígito
    
    # Converte colunas numéricas, tratando erros
    if 'VALANCAMENTO' in chunk.columns:
        chunk['VALANCAMENTO'] = pd.to_numeric(chunk['VALANCAMENTO'], errors='coerce')
        chunk.fillna({'VALANCAMENTO': 0}, inplace=True)
    
    return chunk

def criar_indices(conn):
    """
    Cria todos os índices necessários para otimizar os JOINs.
    """
    print("\nAdicionando índices para otimizar as consultas...")
    cursor = conn.cursor()

    # Índices na tabela FATO (lancamentos)
    print("  - Criando índices na tabela 'lancamentos'...")
    chaves_fato = ['CATEGORIARECEITA', 'ORIGEM', 'ESPECIE', 'ESPECIFICACAO', 'ALINEA', 'FONTE', 'COCONTACONTABIL', 'COUG']
    for chave in chaves_fato:
        try:
            cursor.execute(f'CREATE INDEX idx_lanc_{chave.lower()} ON lancamentos ({chave})')
        except sqlite3.OperationalError as e:
            print(f"    - AVISO ao criar índice em '{chave}': {e}")

    # Índices nas tabelas DIMENSÃO
    print("  - Criando índices nas tabelas de dimensão...")
    for nome_tabela, detalhes in TABELAS_DIMENSAO.items():
        chave_primaria = detalhes['chave_primaria']
        try:
            cursor.execute(f'CREATE INDEX idx_{nome_tabela}_pk ON {nome_tabela} ({chave_primaria})')
        except sqlite3.OperationalError as e:
            print(f"    - AVISO ao criar índice em '{nome_tabela}.{chave_primaria}': {e}")

    conn.commit()

# --- 3. ORQUESTRADOR PRINCIPAL ---

def criar_banco_completo():
    """
    Orquestra todo o processo de ETL.
    """
    start_time = time.time()
    caminho_db = os.path.join(BASE_DIR, 'banco_lancamento_receita.db')

    print(f"Iniciando a criação do banco de dados em '{caminho_db}'...")
    if os.path.exists(caminho_db):
        os.remove(caminho_db)
        print("Banco de dados antigo removido.")

    conn = sqlite3.connect(caminho_db)

    # Carregar Tabela Fato do Excel
    caminho_excel_fato = os.path.join(TABELA_FATO['caminho_relativo'], TABELA_FATO['arquivo_excel'])
    print(f"\n--- Processando Tabela Fato: {TABELA_FATO['arquivo_excel']} ---")
    
    if os.path.exists(caminho_excel_fato):
        print("  - Lendo arquivo Excel...")
        
        try:
            # Primeiro, vamos ver a estrutura do arquivo
            df_amostra = pd.read_excel(caminho_excel_fato, nrows=5)
            print(f"  - Colunas encontradas: {list(df_amostra.columns)}")
            print(f"  - Total de colunas: {len(df_amostra.columns)}")
            
            # Lê o Excel com tipos de dados específicos
            print("  - Carregando arquivo completo...")
            df_completo = pd.read_excel(
                caminho_excel_fato,
                dtype={
                    'COCONTACORRENTE': str,
                    'COCONTACONTABIL': str,
                    'COUG': str
                }
            )
            
            print(f"  - Total de registros: {len(df_completo)}")
            
            # Processa em chunks para melhor performance
            chunk_size = 50000
            total_chunks = (len(df_completo) + chunk_size - 1) // chunk_size
            
            for i in range(0, len(df_completo), chunk_size):
                chunk_num = i // chunk_size + 1
                print(f"  - Processando chunk {chunk_num}/{total_chunks}...")
                chunk = df_completo.iloc[i:i+chunk_size].copy()
                chunk_transformado = transformar_chunk_fato(chunk)
                chunk_transformado.to_sql(TABELA_FATO['nome_tabela'], conn, if_exists='append', index=False)
                
        except Exception as e:
            print(f"ERRO ao processar Excel: {e}")
            return
    else:
        print(f"ERRO: Arquivo '{caminho_excel_fato}' não encontrado!")
        return

    # Carregar Tabelas Dimensão
    print("\n--- Processando Tabelas Dimensão ---")
    caminho_dimensoes = os.path.join(TABELA_FATO['caminho_relativo'], 'dimensao')
    for nome_tabela, detalhes in TABELAS_DIMENSAO.items():
        caminho_csv_dim = os.path.join(caminho_dimensoes, detalhes['arquivo_csv'])
        if os.path.exists(caminho_csv_dim):
            print(f"  - Carregando '{detalhes['arquivo_csv']}' para a tabela '{nome_tabela}'...")
            try:
                df = pd.read_csv(caminho_csv_dim, encoding='latin1', on_bad_lines='skip')
                df.to_sql(nome_tabela, conn, if_exists='replace', index=False)
            except Exception as e:
                print(f"    ERRO ao carregar {detalhes['arquivo_csv']}: {e}")
        else:
            print(f"  - AVISO: Arquivo de dimensão '{caminho_csv_dim}' não encontrado.")

    # Criar Índices
    criar_indices(conn)
    
    # Verifica o resultado
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM lancamentos")
    total = cursor.fetchone()[0]
    print(f"\n✅ Total de registros inseridos na tabela lancamentos: {total}")
    
    conn.close()
    
    end_time = time.time()
    print(f"\nConcluído! Banco de dados '{caminho_db}' criado com sucesso em {end_time - start_time:.2f} segundos.")


if __name__ == '__main__':
    criar_banco_completo()