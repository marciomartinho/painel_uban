# scripts/04_conversor_saldos_despesa.py
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

CAMINHO_DADOS_BRUTOS = os.path.join(BASE_DIR, 'dados', 'dados_brutos')
CAMINHO_DB = os.path.join(BASE_DIR, 'dados', 'db')
os.makedirs(CAMINHO_DB, exist_ok=True)

# Configurações de otimização
CHUNK_SIZE = 50000

COLUNAS_NECESSARIAS = [
    'COEXERCICIO', 'COUG', 'COGESTAO', 'COCONTACONTABIL', 'COCONTACORRENTE',
    'INMES', 'INESFERA', 'COUO', 'COFUNCAO', 'COSUBFUNCAO', 'COPROGRAMA',
    'COPROJETO', 'COSUBTITULO', 'COFONTE', 'CONATUREZA', 'INCATEGORIA',
    'VACREDITO', 'VADEBITO', 'INTIPOADM'
]

# Tipos otimizados
DTYPE_MAP = {
    'COEXERCICIO': 'int16',
    'INMES': 'int8',
    'INTIPOADM': 'int8',
    'INCATEGORIA': 'category',
    'INESFERA': 'category'
}

def processar_valor_monetario_vetorizado(serie):
    """Processa valores monetários de forma vetorizada"""
    mask_str = serie.apply(lambda x: isinstance(x, str))
    
    if mask_str.any():
        valores_str = serie[mask_str].astype(str).str.strip()
        valores_str = valores_str.str.replace('R$', '', regex=False).str.replace('$', '', regex=False).str.strip()
        
        mask_br = valores_str.str.contains(',') & ~valores_str.str.contains('\.')
        mask_us = ~mask_br
        
        valores_str.loc[mask_br] = valores_str.loc[mask_br].str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
        valores_str.loc[mask_us] = valores_str.loc[mask_us].str.replace(',', '', regex=False)
        
        serie.loc[mask_str] = pd.to_numeric(valores_str, errors='coerce')
    
    return pd.to_numeric(serie, errors='coerce').fillna(0).astype('float32')

def extrair_classe_orcamentaria_vetorizado(serie):
    """Extrai classe orçamentária de forma vetorizada"""
    s = serie.astype(str).str.strip()
    mask_40 = s.str.len() == 40
    result = pd.Series([None] * len(serie), index=serie.index)
    result[mask_40] = s[mask_40].str[32:40]
    return result

def processar_chunk(chunk, chunk_num):
    """Processa um chunk de dados"""
    print(f"  - Processando chunk {chunk_num} ({len(chunk):,} registros)...")
    
    # Nomes em minúsculas
    chunk.columns = [col.lower() for col in chunk.columns]
    
    # Processa valores monetários
    for col in ['vadebito', 'vacredito']:
        if col in chunk.columns:
            chunk[col] = processar_valor_monetario_vetorizado(chunk[col])
    
    # Extrai classe orçamentária
    if 'cocontacorrente' in chunk.columns:
        chunk['coclasseorc'] = extrair_classe_orcamentaria_vetorizado(chunk['cocontacorrente'])
    
    # Converte colunas de texto
    colunas_texto = ['coexercicio', 'coug', 'cogestao', 'cocontacontabil', 'cocontacorrente', 
                     'inmes', 'inesfera', 'couo', 'cofuncao', 'cosubfuncao', 'coprograma',
                     'coprojeto', 'cosubtitulo', 'cofonte', 'conatureza', 'incategoria']
    
    for col in colunas_texto:
        if col in chunk.columns and col not in DTYPE_MAP:
            chunk[col] = chunk[col].astype(str)
    
    return chunk

def processar_excel_em_chunks(arquivo_excel, colunas_necessarias, chunk_size=50000):
    """
    Lê arquivo Excel em chunks manualmente com colunas específicas
    """
    print("  - Analisando arquivo Excel...")
    
    # Lê apenas as primeiras linhas para pegar os headers
    df_header = pd.read_excel(arquivo_excel, nrows=0)
    colunas_disponiveis = [col for col in colunas_necessarias if col in df_header.columns]
    
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
                usecols=colunas_disponiveis,
                nrows=chunk_size,
                dtype=DTYPE_MAP
            )
        else:
            # Leituras subsequentes pulam o header e linhas anteriores
            chunk = pd.read_excel(
                arquivo_excel,
                usecols=colunas_disponiveis,
                skiprows=range(1, start_row + 1),
                nrows=chunk_size,
                header=0,
                dtype=DTYPE_MAP
            )
        
        yield chunk, start_row // chunk_size

def processar_saldos_despesa():
    print("=" * 60)
    print("CONVERSOR OTIMIZADO DE SALDOS DE DESPESA")
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

    # Conecta com otimizações
    conn = sqlite3.connect(caminho_db)
    cursor = conn.cursor()
    
    # Configurações de performance
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA synchronous=NORMAL")
    cursor.execute("PRAGMA cache_size=10000")
    cursor.execute("PRAGMA temp_store=MEMORY")

    try:
        # Informações do arquivo
        file_size = os.path.getsize(arquivo_excel)
        print(f"  - Arquivo de {file_size / 1024 / 1024:.1f} MB")
        print(f"  - Processamento em chunks de {CHUNK_SIZE:,} registros...")
        
        first_chunk = True
        total_processed = 0
        stats = {
            'debito_total': 0,
            'credito_total': 0,
            'count_debito': 0,
            'count_credito': 0
        }
        
        # Processa em chunks
        for chunk, chunk_num in processar_excel_em_chunks(arquivo_excel, COLUNAS_NECESSARIAS, CHUNK_SIZE):
            # Processa chunk
            chunk_processado = processar_chunk(chunk, chunk_num)
            
            # Estatísticas
            if 'vadebito' in chunk_processado.columns:
                debitos = chunk_processado['vadebito']
                stats['debito_total'] += debitos.sum()
                stats['count_debito'] += (debitos > 0).sum()
            
            if 'vacredito' in chunk_processado.columns:
                creditos = chunk_processado['vacredito']
                stats['credito_total'] += creditos.sum()
                stats['count_credito'] += (creditos > 0).sum()
            
            # Salva no banco
            if first_chunk:
                chunk_processado.to_sql('fato_saldo_despesa', conn, if_exists='replace', index=False)
                first_chunk = False
            else:
                chunk_processado.to_sql('fato_saldo_despesa', conn, if_exists='append', index=False)
            
            total_processed += len(chunk)
            
            # Progresso
            elapsed = time.time() - start_time
            rate = total_processed / elapsed if elapsed > 0 else 0
            print(f"    ✓ Total: {total_processed:,} registros ({rate:.0f} registros/seg)")
            
            # Commit periódico
            if chunk_num % 10 == 0:
                conn.commit()
        
        print(f"\n  📊 Estatísticas finais:")
        print(f"     Total de registros: {total_processed:,}")
        print(f"     Registros com débito: {stats['count_debito']:,}")
        print(f"     Registros com crédito: {stats['count_credito']:,}")
        print(f"     Total débito: R$ {stats['debito_total']:,.2f}")
        print(f"     Total crédito: R$ {stats['credito_total']:,.2f}")
        
        print("\n  - Criando índices otimizados...")
        
        indices = [
            ("idx_saldo_desp_periodo", "coexercicio, inmes"),
            ("idx_saldo_desp_ug", "coug"),
            ("idx_saldo_desp_conta", "cocontacontabil"),
            ("idx_saldo_desp_fonte", "cofonte"),
            ("idx_saldo_desp_natureza", "conatureza"),
            ("idx_saldo_desp_classe", "coclasseorc")
        ]
        
        for idx_name, idx_cols in indices:
            print(f"    - Criando {idx_name}...")
            cursor.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON fato_saldo_despesa ({idx_cols})")
        
        # Otimização final
        print("\n  - Otimizando banco de dados...")
        cursor.execute("ANALYZE")
        cursor.execute("VACUUM")
        
        conn.commit()
        conn.close()
        
        end_time = time.time()
        tempo_total = end_time - start_time
        
        print(f"\n✅ Processamento concluído!")
        print(f"   Tempo total: {tempo_total:.2f} segundos")
        print(f"   Taxa média: {total_processed/tempo_total:.0f} registros/segundo")

    except Exception as e:
        print(f"\n❌ ERRO durante o processamento: {e}")
        import traceback
        traceback.print_exc()
        conn.close()

if __name__ == "__main__":
    processar_saldos_despesa()