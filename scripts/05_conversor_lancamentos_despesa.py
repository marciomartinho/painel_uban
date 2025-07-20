# scripts/05_conversor_lancamentos_despesa.py
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
CHUNK_SIZE = 50000  # Processar 50k registros por vez

# Colunas a serem lidas do Excel
COLUNAS_NECESSARIAS = [
    'COEXERCICIO', 'COUG', 'COGESTAO', 'NUDOCUMENTO', 'COEVENTO', 'COCONTACONTABIL',
    'COCONTACORRENTE', 'INMES', 'DALANCAMENTO', 'VALANCAMENTO', 'INDEBITOCREDITO',
    'INABREENCERRA', 'COUGDESTINO', 'COGESTAODESTINO', 'DATRANSACAO', 'COUGCONTAB', 'COGESTAOCONTAB'
]

# Tipos otimizados
DTYPE_MAP = {
    'COEXERCICIO': 'int16',
    'INMES': 'int8',
    'INDEBITOCREDITO': 'category',
    'INABREENCERRA': 'category'
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

def extrair_campos_despesa_vetorizado(df):
    """Extrai campos orçamentários de forma vetorizada - MUITO mais rápida"""
    cc = df['cocontacorrente'].astype(str).str.strip()
    
    # Máscaras para tamanhos válidos
    mask_38_40 = (cc.str.len() == 38) | (cc.str.len() == 40)
    mask_40 = cc.str.len() == 40
    
    # Inicializa colunas
    campos = ['inesfera', 'couo', 'cofuncao', 'cosubfuncao', 'coprograma', 
              'coprojeto', 'cosubtitulo', 'cofonte', 'conatureza', 'incategoria', 
              'cogrupo', 'comodalidade', 'coelemento', 'subelemento', 'coclasseorc']
    
    for campo in campos:
        df[campo] = None
    
    # Extração vetorizada
    df.loc[mask_38_40, 'inesfera'] = cc[mask_38_40].str[0]
    df.loc[mask_38_40, 'couo'] = cc[mask_38_40].str[1:6]
    df.loc[mask_38_40, 'cofuncao'] = cc[mask_38_40].str[6:8]
    df.loc[mask_38_40, 'cosubfuncao'] = cc[mask_38_40].str[8:11]
    df.loc[mask_38_40, 'coprograma'] = cc[mask_38_40].str[11:15]
    df.loc[mask_38_40, 'coprojeto'] = cc[mask_38_40].str[15:19]
    df.loc[mask_38_40, 'cosubtitulo'] = cc[mask_38_40].str[19:23]
    df.loc[mask_38_40, 'cofonte'] = cc[mask_38_40].str[23:32]
    df.loc[mask_38_40, 'conatureza'] = cc[mask_38_40].str[32:38]
    df.loc[mask_38_40, 'incategoria'] = cc[mask_38_40].str[32]
    df.loc[mask_38_40, 'cogrupo'] = cc[mask_38_40].str[33]
    df.loc[mask_38_40, 'comodalidade'] = cc[mask_38_40].str[34:36]
    df.loc[mask_38_40, 'coelemento'] = cc[mask_38_40].str[36:38]
    
    # Campos específicos para 40 caracteres
    df.loc[mask_40, 'subelemento'] = cc[mask_40].str[38:40]
    df.loc[mask_40, 'coclasseorc'] = cc[mask_40].str[32:40]
    
    return df

def processar_chunk(chunk, chunk_num):
    """Processa um chunk de dados"""
    print(f"  - Processando chunk {chunk_num} ({len(chunk):,} registros)...")
    
    # Nomes em minúsculas
    chunk.columns = [col.lower() for col in chunk.columns]
    
    # Processa valores monetários
    if 'valancamento' in chunk.columns:
        chunk['valancamento'] = processar_valor_monetario_vetorizado(chunk['valancamento'])
    
    # Extrai campos orçamentários
    if 'cocontacorrente' in chunk.columns:
        chunk = extrair_campos_despesa_vetorizado(chunk)
    
    # Converte colunas de texto
    colunas_texto = [col for col in chunk.columns if col not in ['valancamento', 'vadebito', 'vacredito']]
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

def processar_lancamentos_despesa():
    print("=" * 60)
    print("CONVERSOR OTIMIZADO DE LANÇAMENTOS DE DESPESA")
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
        estimated_rows = int(file_size / 800)  # Estimativa: ~800 bytes por linha
        print(f"  - Estimando ~{estimated_rows:,} registros")
        print(f"  - Processamento em chunks de {CHUNK_SIZE:,} registros...")
        
        first_chunk = True
        total_processed = 0
        stats = {
            'valor_min': float('inf'),
            'valor_max': float('-inf'),
            'valor_soma': 0,
            'total_debitos': 0,
            'total_creditos': 0,
            'count_debitos': 0,
            'count_creditos': 0
        }
        
        # Processa em chunks
        for chunk, chunk_num in processar_excel_em_chunks(arquivo_excel, COLUNAS_NECESSARIAS, CHUNK_SIZE):
            # Processa chunk
            chunk_processado = processar_chunk(chunk, chunk_num)
            
            # Estatísticas
            if 'valancamento' in chunk_processado.columns and 'indebitocredito' in chunk_processado.columns:
                valores = chunk_processado['valancamento']
                dc = chunk_processado['indebitocredito']
                
                valores_nao_zero = valores[valores != 0]
                if len(valores_nao_zero) > 0:
                    stats['valor_min'] = min(stats['valor_min'], valores_nao_zero.min())
                    stats['valor_max'] = max(stats['valor_max'], valores_nao_zero.max())
                    stats['valor_soma'] += valores_nao_zero.sum()
                
                # Soma débitos e créditos
                mask_debito = dc == 'D'
                mask_credito = dc == 'C'
                
                stats['total_debitos'] += valores[mask_debito].sum()
                stats['total_creditos'] += valores[mask_credito].sum()
                stats['count_debitos'] += mask_debito.sum()
                stats['count_creditos'] += mask_credito.sum()
            
            # Salva no banco
            if first_chunk:
                chunk_processado.to_sql('fato_lancamento_despesa', conn, if_exists='replace', index=False)
                first_chunk = False
            else:
                chunk_processado.to_sql('fato_lancamento_despesa', conn, if_exists='append', index=False)
            
            total_processed += len(chunk)
            
            # Progresso
            elapsed = time.time() - start_time
            rate = total_processed / elapsed if elapsed > 0 else 0
            eta = (estimated_rows - total_processed) / rate if rate > 0 else 0
            
            print(f"    ✓ Total: {total_processed:,} registros ({rate:.0f} registros/seg)")
            if eta > 0 and total_processed < estimated_rows:
                print(f"      Tempo estimado restante: {eta/60:.1f} minutos")
            
            # Commit periódico
            if chunk_num % 10 == 0:
                conn.commit()
        
        print(f"\n  📊 Estatísticas finais:")
        print(f"     Total de registros: {total_processed:,}")
        print(f"     Lançamentos a débito: {stats['count_debitos']:,}")
        print(f"     Lançamentos a crédito: {stats['count_creditos']:,}")
        print(f"     Total débitos: R$ {stats['total_debitos']:,.2f}")
        print(f"     Total créditos: R$ {stats['total_creditos']:,.2f}")
        print(f"     Saldo líquido (D-C): R$ {stats['total_debitos'] - stats['total_creditos']:,.2f}")
        
        if stats['valor_min'] != float('inf'):
            print(f"     Menor lançamento: R$ {stats['valor_min']:,.2f}")
            print(f"     Maior lançamento: R$ {stats['valor_max']:,.2f}")
        
        print("\n  - Criando índices otimizados...")
        
        indices = [
            ("idx_lanc_desp_periodo", "coexercicio, inmes"),
            ("idx_lanc_desp_ug", "coug"),
            ("idx_lanc_desp_conta", "cocontacontabil"),
            ("idx_lanc_desp_fonte", "cofonte"),
            ("idx_lanc_desp_natureza", "conatureza"),
            ("idx_lanc_desp_evento", "coevento"),
            ("idx_lanc_desp_valor", "valancamento"),
            ("idx_lanc_desp_dc", "indebitocredito")
        ]
        
        for idx_name, idx_cols in indices:
            print(f"    - Criando {idx_name}...")
            cursor.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON fato_lancamento_despesa ({idx_cols})")
        
        # Otimização final
        print("\n  - Otimizando banco de dados...")
        cursor.execute("ANALYZE")
        cursor.execute("VACUUM")
        
        conn.commit()
        conn.close()
        
        end_time = time.time()
        tempo_total = end_time - start_time
        
        print(f"\n✅ Processamento concluído!")
        print(f"   Tempo total: {tempo_total:.2f} segundos ({tempo_total/60:.1f} minutos)")
        print(f"   Taxa média: {total_processed/tempo_total:.0f} registros/segundo")

    except Exception as e:
        print(f"\n❌ ERRO durante o processamento: {e}")
        import traceback
        traceback.print_exc()
        conn.close()

if __name__ == "__main__":
    processar_lancamentos_despesa()