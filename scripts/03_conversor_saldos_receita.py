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
CHUNK_SIZE = 50000

# Tipos de dados otimizados
DTYPE_MAP = {
    'COEXERCICIO': 'int16',
    'INMES': 'int8',
    'INTIPOADM': 'int8'
}

def processar_valor_monetario_vetorizado(serie):
    """Processa valores monetários de forma vetorizada"""
    # Converte para string apenas os não-numéricos
    mask_str = serie.apply(lambda x: isinstance(x, str))
    
    if mask_str.any():
        valores_str = serie[mask_str].astype(str).str.strip()
        valores_str = valores_str.str.replace('R$', '', regex=False).str.replace('$', '', regex=False).str.strip()
        
        # Detecta formato brasileiro
        mask_br = valores_str.str.contains(',') & ~valores_str.str.contains('\.')
        mask_us = ~mask_br
        
        # Processa cada formato
        valores_str.loc[mask_br] = valores_str.loc[mask_br].str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
        valores_str.loc[mask_us] = valores_str.loc[mask_us].str.replace(',', '', regex=False)
        
        serie.loc[mask_str] = pd.to_numeric(valores_str, errors='coerce')
    
    return pd.to_numeric(serie, errors='coerce').fillna(0).astype('float32')

def calcular_saldo_contabil_vetorizado(df):
    """Calcula saldo contábil de forma vetorizada"""
    conta_str = df['cocontacontabil'].astype(str).str.strip()
    
    # Máscaras para tipos de conta
    mask_5 = conta_str.str.startswith('5')
    mask_6 = conta_str.str.startswith('6')
    
    # Calcula saldo baseado no tipo de conta
    saldo = np.zeros(len(df), dtype='float32')
    saldo[mask_5] = df.loc[mask_5, 'vadebito'] - df.loc[mask_5, 'vacredito']
    saldo[mask_6] = df.loc[mask_6, 'vacredito'] - df.loc[mask_6, 'vadebito']
    
    return saldo

def extrair_campos_cocontacorrente_vetorizado(df):
    """Extrai campos do cocontacorrente de forma vetorizada"""
    cc = df['cocontacorrente'].astype(str).str.strip()
    
    df['categoriareceita'] = cc.str[0:1]
    df['cofontereceita'] = cc.str[0:2]
    df['cosubfontereceita'] = cc.str[0:3]
    df['corubrica'] = cc.str[0:4]
    df['coalinea'] = cc.str[0:6]
    df['cofonte'] = cc.str[8:17]
    
    return df

def processar_chunk(chunk, chunk_num):
    """Processa um chunk de dados"""
    print(f"  - Processando chunk {chunk_num} ({len(chunk):,} registros)...")
    
    # Nomes em minúsculas
    chunk.columns = [col.lower() for col in chunk.columns]
    
    # Extrai campos
    if 'cocontacorrente' in chunk.columns:
        chunk = extrair_campos_cocontacorrente_vetorizado(chunk)
    
    # Processa valores monetários
    for col in ['vadebito', 'vacredito']:
        if col in chunk.columns:
            chunk[col] = processar_valor_monetario_vetorizado(chunk[col])
    
    # Calcula saldo contábil
    if all(col in chunk.columns for col in ['cocontacontabil', 'vadebito', 'vacredito']):
        chunk['saldo_contabil'] = calcular_saldo_contabil_vetorizado(chunk)
    
    return chunk

def processar_excel_em_chunks(arquivo_excel, chunk_size=50000):
    """
    Lê arquivo Excel em chunks manualmente
    """
    print("  - Analisando arquivo Excel...")
    
    # Lê apenas as primeiras linhas para pegar os headers
    df_header = pd.read_excel(arquivo_excel, nrows=0)
    colunas = df_header.columns.tolist()
    
    # Conta o número total de linhas
    print("  - Contando total de registros...")
    df_count = pd.read_excel(arquivo_excel, usecols=[0])
    total_rows = len(df_count)
    print(f"  - Total de registros: {total_rows:,}")
    del df_count  # Libera memória
    
    # Processa em chunks
    for start_row in range(0, total_rows, chunk_size):
        end_row = min(start_row + chunk_size, total_rows)
        
        print(f"\n  - Lendo linhas {start_row:,} a {end_row:,}...")
        
        if start_row == 0:
            # Primeira leitura inclui header
            chunk = pd.read_excel(
                arquivo_excel,
                nrows=chunk_size,
                dtype=DTYPE_MAP
            )
        else:
            # Leituras subsequentes pulam o header e linhas anteriores
            chunk = pd.read_excel(
                arquivo_excel,
                skiprows=range(1, start_row + 1),
                nrows=chunk_size,
                header=0,
                dtype=DTYPE_MAP
            )
        
        yield chunk, start_row // chunk_size

def processar_saldos():
    """Processa o arquivo de saldos com otimizações"""
    print("=" * 60)
    print("CONVERSOR OTIMIZADO DE SALDOS DE RECEITA")
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
    
    # Conecta com otimizações
    conn = sqlite3.connect(caminho_db)
    cursor = conn.cursor()
    
    # Configurações de performance
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA synchronous=NORMAL")
    cursor.execute("PRAGMA cache_size=10000")
    cursor.execute("PRAGMA temp_store=MEMORY")
    
    print("\n--- Processando Saldos ---")
    
    try:
        # Estima tamanho
        file_size = os.path.getsize(arquivo_excel)
        print(f"  - Arquivo de {file_size / 1024 / 1024:.1f} MB")
        print(f"  - Processamento em chunks de {CHUNK_SIZE:,} registros...")
        
        first_chunk = True
        total_processed = 0
        stats = {
            'saldo_min': float('inf'),
            'saldo_max': float('-inf'),
            'saldo_soma': 0,
            'count_positivo': 0,
            'count_negativo': 0
        }
        
        # Processa em chunks
        for chunk, chunk_num in processar_excel_em_chunks(arquivo_excel, CHUNK_SIZE):
            # Processa chunk
            chunk_processado = processar_chunk(chunk, chunk_num)
            
            # Coleta estatísticas
            if 'saldo_contabil' in chunk_processado.columns:
                saldos_nao_zero = chunk_processado[chunk_processado['saldo_contabil'] != 0]['saldo_contabil']
                if len(saldos_nao_zero) > 0:
                    stats['saldo_min'] = min(stats['saldo_min'], saldos_nao_zero.min())
                    stats['saldo_max'] = max(stats['saldo_max'], saldos_nao_zero.max())
                    stats['saldo_soma'] += saldos_nao_zero.sum()
                    stats['count_positivo'] += (saldos_nao_zero > 0).sum()
                    stats['count_negativo'] += (saldos_nao_zero < 0).sum()
            
            # Salva no banco
            if first_chunk:
                chunk_processado.to_sql('fato_saldos', conn, if_exists='replace', index=False)
                first_chunk = False
            else:
                chunk_processado.to_sql('fato_saldos', conn, if_exists='append', index=False)
            
            total_processed += len(chunk)
            
            # Progresso
            elapsed = time.time() - start_time
            rate = total_processed / elapsed if elapsed > 0 else 0
            print(f"    ✓ Total: {total_processed:,} registros ({rate:.0f} registros/seg)")
            
            # Commit periódico
            if chunk_num % 10 == 0:
                conn.commit()
        
        print(f"\n  📊 Estatísticas dos saldos:")
        if stats['saldo_min'] != float('inf'):
            print(f"     Menor saldo: R$ {stats['saldo_min']:,.2f}")
            print(f"     Maior saldo: R$ {stats['saldo_max']:,.2f}")
            print(f"     Soma total: R$ {stats['saldo_soma']:,.2f}")
            print(f"     Saldos positivos: {stats['count_positivo']:,}")
            print(f"     Saldos negativos: {stats['count_negativo']:,}")
        
        print("\n  - Criando tabela dim_tempo...")
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
        
        print("\n  - Criando índices otimizados...")
        
        indices = [
            ("idx_saldo_periodo", "coexercicio, inmes"),
            ("idx_saldo_alinea", "coalinea"),
            ("idx_saldo_fonte", "cofonte"),
            ("idx_saldo_conta", "cocontacontabil"),
            ("idx_saldo_ug", "coug"),
            ("idx_saldo_valor", "saldo_contabil"),
            ("idx_tempo_periodo", "coexercicio, inmes", "dim_tempo")
        ]
        
        for idx_info in indices:
            idx_name = idx_info[0]
            idx_cols = idx_info[1]
            table_name = idx_info[2] if len(idx_info) > 2 else "fato_saldos"
            print(f"    - Criando {idx_name}...")
            cursor.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table_name} ({idx_cols})")
        
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
    processar_saldos()