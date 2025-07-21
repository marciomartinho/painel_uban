import pandas as pd
import sqlite3
import os
import time
import numpy as np

# --- CONFIGURAÇÃO ---
if os.path.basename(os.getcwd()) == 'scripts':
    BASE_DIR = os.path.dirname(os.getcwd())
else:
    BASE_DIR = os.getcwd()

# Caminhos
CAMINHO_DADOS_BRUTOS = os.path.join(BASE_DIR, 'dados', 'dados_brutos')
CAMINHO_DB = os.path.join(BASE_DIR, 'dados', 'db')

os.makedirs(CAMINHO_DB, exist_ok=True)

# Configurações de otimização
CHUNK_SIZE = 50000  # Processar 50k linhas por vez

# Colunas necessárias do Excel
COLUNAS_EXCEL = [
    'COEXERCICIO', 'COUG', 'NUDOCUMENTO', 'COEVENTO', 'COCONTACONTABIL',
    'INMES', 'VALANCAMENTO', 'INDEBITOCREDITO', 'COUGCONTAB', 'COCONTACORRENTE'
]

# Tipos de dados otimizados
DTYPE_MAP = {
    'COEXERCICIO': 'int16',
    'INMES': 'int8',
    'INDEBITOCREDITO': 'category'
}

def processar_valor_monetario_vetorizado(serie):
    """
    Processa valores monetários de forma vetorizada (mais rápida)
    """
    # Converte para string apenas os não-numéricos
    mask_str = serie.apply(lambda x: isinstance(x, str))
    
    # Processa valores string
    if mask_str.any():
        valores_str = serie[mask_str].astype(str).str.strip()
        valores_str = valores_str.str.replace('R$', '', regex=False)
        valores_str = valores_str.str.replace('$', '', regex=False)
        valores_str = valores_str.str.strip()
        
        # Detecta formato brasileiro (vírgula como decimal)
        mask_br = valores_str.str.contains(',') & ~valores_str.str.contains('\.')
        mask_us = ~mask_br
        
        # Processa formato brasileiro
        valores_str.loc[mask_br] = valores_str.loc[mask_br].str.replace('.', '', regex=False)
        valores_str.loc[mask_br] = valores_str.loc[mask_br].str.replace(',', '.', regex=False)
        
        # Processa formato americano
        valores_str.loc[mask_us] = valores_str.loc[mask_us].str.replace(',', '', regex=False)
        
        # Converte para float
        serie.loc[mask_str] = pd.to_numeric(valores_str, errors='coerce')
    
    # Converte valores numéricos diretos
    return pd.to_numeric(serie, errors='coerce').fillna(0).astype('float32')

def extrair_campos_cocontacorrente_vetorizado(df):
    """
    Extrai campos do cocontacorrente de forma vetorizada,
    tratando os formatos de 17 e 38 dígitos.
    """
    print("  - Extraindo campos do cocontacorrente (vetorizado)...")
    
    cc = df['cocontacorrente'].astype(str).str.strip()
    
    # --- Inicializa todas as colunas possíveis com valores nulos ---
    # Colunas da regra de 17 dígitos
    df['categoriareceita'] = pd.NA
    df['cofontereceita'] = pd.NA
    df['cosubfontereceita'] = pd.NA
    df['corubrica'] = pd.NA
    df['coalinea'] = pd.NA
    # Colunas da regra de 38 dígitos
    df['inesfera'] = pd.NA
    df['couo'] = pd.NA
    df['cofuncao'] = pd.NA
    df['cosubfuncao'] = pd.NA
    df['coprograma'] = pd.NA
    df['coprojeto'] = pd.NA
    df['cosubtitulo'] = pd.NA
    df['conatureza'] = pd.NA
    df['incategoria'] = pd.NA
    df['cogrupo'] = pd.NA
    df['comodalidade'] = pd.NA
    df['coelemento'] = pd.NA
    # Coluna comum
    df['cofonte'] = pd.NA

    # --- Cria máscaras para identificar o tamanho do código ---
    mask_17 = cc.str.len() == 17
    mask_38 = cc.str.len() == 38
    
    # --- Aplica a regra para códigos de 17 dígitos ---
    if mask_17.any():
        cc_17 = cc[mask_17]
        df.loc[mask_17, 'categoriareceita'] = cc_17.str[0:1]
        df.loc[mask_17, 'cofontereceita'] = cc_17.str[0:2]
        df.loc[mask_17, 'cosubfontereceita'] = cc_17.str[0:3]
        df.loc[mask_17, 'corubrica'] = cc_17.str[0:4]
        df.loc[mask_17, 'coalinea'] = cc_17.str[0:6]
        df.loc[mask_17, 'cofonte'] = cc_17.str[8:17]

    # --- Aplica a regra para códigos de 38 dígitos ---
    if mask_38.any():
        cc_38 = cc[mask_38]
        df.loc[mask_38, 'inesfera'] = cc_38.str[0:1]
        df.loc[mask_38, 'couo'] = cc_38.str[1:6]
        df.loc[mask_38, 'cofuncao'] = cc_38.str[6:8]
        df.loc[mask_38, 'cosubfuncao'] = cc_38.str[8:11]
        df.loc[mask_38, 'coprograma'] = cc_38.str[11:15]
        df.loc[mask_38, 'coprojeto'] = cc_38.str[15:19]
        df.loc[mask_38, 'cosubtitulo'] = cc_38.str[19:23]
        df.loc[mask_38, 'cofonte'] = cc_38.str[23:32]  # Sobrescreve a coluna cofonte
        df.loc[mask_38, 'conatureza'] = cc_38.str[32:38]
        df.loc[mask_38, 'incategoria'] = cc_38.str[32:33]
        df.loc[mask_38, 'cogrupo'] = cc_38.str[33:34]
        df.loc[mask_38, 'comodalidade'] = cc_38.str[34:36]
        df.loc[mask_38, 'coelemento'] = cc_38.str[36:38]
        
    df = df.drop('cocontacorrente', axis=1, errors='ignore')
    print("    ✅ Campos extraídos!")
    return df

def processar_chunk(chunk, chunk_num):
    """Processa um chunk de dados"""
    print(f"  - Processando chunk {chunk_num} ({len(chunk):,} registros)...")
    
    # Converte colunas para minúsculas
    chunk.columns = [col.lower() for col in chunk.columns]
    
    # Processa valores monetários de forma vetorizada
    if 'valancamento' in chunk.columns:
        chunk['valancamento'] = processar_valor_monetario_vetorizado(chunk['valancamento'])
    
    # Extrai campos do cocontacorrente
    if 'cocontacorrente' in chunk.columns:
        chunk = extrair_campos_cocontacorrente_vetorizado(chunk)
    
    return chunk

def processar_excel_em_chunks(arquivo_excel, chunk_size=50000):
    """
    Lê arquivo Excel em chunks manualmente
    """
    # Primeiro, descobre o número total de linhas
    print("  - Analisando arquivo Excel...")
    
    # Lê apenas as primeiras linhas para pegar os headers
    df_header = pd.read_excel(arquivo_excel, nrows=0)
    colunas_disponiveis = [col for col in COLUNAS_EXCEL if col in df_header.columns]
    
    # Conta o número total de linhas (pode demorar um pouco)
    print("  - Contando total de registros...")
    df_count = pd.read_excel(arquivo_excel, usecols=[0])  # Lê apenas primeira coluna para contar
    total_rows = len(df_count)
    print(f"  - Total de registros: {total_rows:,}")
    
    # Processa em chunks
    chunks_processados = []
    for start_row in range(0, total_rows, chunk_size):
        end_row = min(start_row + chunk_size, total_rows)
        
        print(f"\n  - Lendo linhas {start_row:,} a {end_row:,}...")
        
        # Lê o chunk
        if start_row == 0:
            # Primeira leitura inclui header
            chunk = pd.read_excel(
                arquivo_excel,
                usecols=colunas_disponiveis,
                nrows=chunk_size,
                dtype=DTYPE_MAP
            )
        else:
            # Leituras subsequentes pulam o header
            chunk = pd.read_excel(
                arquivo_excel,
                usecols=colunas_disponiveis,
                skiprows=range(1, start_row + 1),  # Pula header + linhas anteriores
                nrows=chunk_size,
                header=0,
                dtype=DTYPE_MAP
            )
        
        yield chunk, start_row // chunk_size

def processar_lancamentos():
    """Processa o arquivo de lançamentos com otimizações"""
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
    
    # Conecta com otimizações
    conn = sqlite3.connect(caminho_db)
    cursor = conn.cursor()
    
    # Configurações de performance do SQLite
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA synchronous=NORMAL")
    cursor.execute("PRAGMA cache_size=10000")
    cursor.execute("PRAGMA temp_store=MEMORY")
    
    print("\n--- Processando Lançamentos ---")
    
    try:
        # Estima tamanho do arquivo
        file_size = os.path.getsize(arquivo_excel)
        print(f"  - Arquivo de {file_size / 1024 / 1024:.1f} MB")
        
        first_chunk = True
        total_processed = 0
        valores_min = float('inf')
        valores_max = float('-inf')
        valores_soma = 0
        
        # Processa em chunks
        for chunk, chunk_num in processar_excel_em_chunks(arquivo_excel, CHUNK_SIZE):
            # Processa o chunk
            chunk_processado = processar_chunk(chunk, chunk_num)
            
            # Estatísticas
            if 'valancamento' in chunk_processado.columns:
                valores_chunk = chunk_processado['valancamento']
                valores_nao_zero = valores_chunk[valores_chunk != 0]
                if len(valores_nao_zero) > 0:
                    valores_min = min(valores_min, valores_nao_zero.min())
                    valores_max = max(valores_max, valores_nao_zero.max())
                    valores_soma += valores_nao_zero.sum()
            
            # Salva no banco
            if first_chunk:
                chunk_processado.to_sql('lancamentos', conn, if_exists='replace', index=False)
                first_chunk = False
            else:
                chunk_processado.to_sql('lancamentos', conn, if_exists='append', index=False)
            
            total_processed += len(chunk)
            
            # Mostra progresso
            elapsed = time.time() - start_time
            rate = total_processed / elapsed if elapsed > 0 else 0
            print(f"    ✓ Total processado: {total_processed:,} registros ({rate:.0f} registros/seg)")
            
            # Commit periodicamente
            if chunk_num % 5 == 0:
                conn.commit()
        
        print(f"\n  📊 Estatísticas dos valores:")
        if valores_min != float('inf'):
            print(f"     Menor valor: R$ {valores_min:,.2f}")
            print(f"     Maior valor: R$ {valores_max:,.2f}")
            print(f"     Soma total: R$ {valores_soma:,.2f}")
        
        print("\n  - Criando índices otimizados...")
        
        # Desabilita temporariamente algumas verificações
        cursor.execute("PRAGMA foreign_keys=OFF")
        
        indices = [
            ("idx_lancamento_periodo", "coexercicio, inmes"),
            ("idx_lancamento_alinea", "coalinea"),
            ("idx_lancamento_fonte", "cofonte"),
            ("idx_lancamento_conta", "cocontacontabil"),
            ("idx_lancamento_ug", "coug"),
            ("idx_lancamento_valor", "valancamento")
        ]
        
        for idx_name, idx_cols in indices:
            print(f"    - Criando {idx_name}...")
            cursor.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON lancamentos ({idx_cols})")
        
        # Otimização final
        print("\n  - Otimizando banco de dados...")
        cursor.execute("ANALYZE")
        cursor.execute("VACUUM")
        
        conn.commit()
        conn.close()
        
        end_time = time.time()
        tempo_total = end_time - start_time
        
        print(f"\n✅ Processamento concluído!")
        print(f"   Total de registros: {total_processed:,}")
        print(f"   Tempo total: {tempo_total:.2f} segundos")
        print(f"   Taxa média: {total_processed/tempo_total:.0f} registros/segundo")
        
    except Exception as e:
        print(f"\n❌ ERRO durante o processamento: {e}")
        import traceback
        traceback.print_exc()
        conn.close()

if __name__ == "__main__":
    processar_lancamentos()