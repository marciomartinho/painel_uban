import pandas as pd
import sqlite3
import os
import time
import sys

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

def extrair_campos_cocontacorrente(df):
    """
    Extrai os campos do COCONTACORRENTE conforme as regras:
    - 1º dígito = CATEGORIARECEITA
    - 2 primeiros = COFONTERECEITA
    - 3 primeiros = COSUBFONTERECEITA
    - 4 primeiros = CORUBRICA
    - 6 primeiros = COALINEA
    - 9º ao 17º = COFONTE
    """
    print("  - Extraindo campos do COCONTACORRENTE...")
    
    # Garante que COCONTACORRENTE seja string
    df['COCONTACORRENTE'] = df['COCONTACORRENTE'].astype(str).str.strip()
    
    # Mostra exemplo
    if len(df) > 0:
        exemplo = df['COCONTACORRENTE'].iloc[0]
        print(f"    Exemplo COCONTACORRENTE: '{exemplo}' (tamanho: {len(exemplo)})")
    
    # Extrai os campos
    df['CATEGORIARECEITA'] = df['COCONTACORRENTE'].str[0:1]      # 1º dígito
    df['COFONTERECEITA'] = df['COCONTACORRENTE'].str[0:2]        # 2 primeiros
    df['COSUBFONTERECEITA'] = df['COCONTACORRENTE'].str[0:3]     # 3 primeiros
    df['CORUBRICA'] = df['COCONTACORRENTE'].str[0:4]             # 4 primeiros
    df['COALINEA'] = df['COCONTACORRENTE'].str[0:6]              # 6 primeiros
    df['COFONTE'] = df['COCONTACORRENTE'].str[8:17]              # 9º ao 17º
    
    print("    ✅ Campos extraídos com sucesso!")
    
    # Mostra exemplo dos campos extraídos
    if len(df) > 0:
        print(f"    Exemplo de extração:")
        print(f"      CATEGORIARECEITA: {df['CATEGORIARECEITA'].iloc[0]}")
        print(f"      COFONTERECEITA: {df['COFONTERECEITA'].iloc[0]}")
        print(f"      COSUBFONTERECEITA: {df['COSUBFONTERECEITA'].iloc[0]}")
        print(f"      CORUBRICA: {df['CORUBRICA'].iloc[0]}")
        print(f"      COALINEA: {df['COALINEA'].iloc[0]}")
        print(f"      COFONTE: {df['COFONTE'].iloc[0]}")
    
    return df

def processar_lancamentos():
    """Processa o arquivo de lançamentos e cria o banco de dados"""
    
    print("=" * 60)
    print("CONVERSOR DE LANÇAMENTOS DE RECEITA")
    print("=" * 60)
    
    start_time = time.time()
    
    # Arquivos
    arquivo_excel = os.path.join(CAMINHO_DADOS_BRUTOS, 'ReceitaLancamento.xlsx')
    caminho_db = os.path.join(CAMINHO_DB, 'banco_lancamento_receita.db')
    
    print(f"\nArquivo fonte: {arquivo_excel}")
    print(f"Banco destino: {caminho_db}")
    
    # Verifica se o arquivo existe
    if not os.path.exists(arquivo_excel):
        print(f"\n❌ ERRO: Arquivo '{arquivo_excel}' não encontrado!")
        return
    
    # Verifica se o banco já existe
    if os.path.exists(caminho_db):
        resposta = input("\nBanco de lançamentos já existe. Deseja substituí-lo? (s/n): ")
        if resposta.lower() != 's':
            print("Operação cancelada.")
            return
        os.remove(caminho_db)
        print("Banco antigo removido.")
    
    print("\n--- Processando Lançamentos ---")
    
    try:
        # Lê o Excel
        print("  - Lendo arquivo Excel...")
        df = pd.read_excel(arquivo_excel, dtype={
            'COCONTACORRENTE': str,
            'COCONTACONTABIL': str,
            'COUG': str
        })
        
        print(f"  - Total de registros: {len(df):,}")
        print(f"  - Colunas encontradas: {', '.join(df.columns.tolist())}")
        
        # Extrai campos do COCONTACORRENTE
        df = extrair_campos_cocontacorrente(df)
        
        # Converte valores numéricos
        if 'VALANCAMENTO' in df.columns:
            df['VALANCAMENTO'] = pd.to_numeric(df['VALANCAMENTO'], errors='coerce').fillna(0)
            print(f"  - Coluna VALANCAMENTO convertida para numérico")
        
        # Conecta ao banco
        conn = sqlite3.connect(caminho_db)
        
        # Salva no banco
        print("\n  - Salvando no banco de dados...")
        df.to_sql('lancamentos', conn, if_exists='replace', index=False, chunksize=10000)
        
        # Cria índices
        print("\n  - Criando índices...")
        cursor = conn.cursor()
        
        indices = [
            "CREATE INDEX idx_lanc_categoriareceita ON lancamentos (CATEGORIARECEITA)",
            "CREATE INDEX idx_lanc_cofontereceita ON lancamentos (COFONTERECEITA)",
            "CREATE INDEX idx_lanc_cosubfontereceita ON lancamentos (COSUBFONTERECEITA)",
            "CREATE INDEX idx_lanc_corubrica ON lancamentos (CORUBRICA)",
            "CREATE INDEX idx_lanc_coalinea ON lancamentos (COALINEA)",
            "CREATE INDEX idx_lanc_cofonte ON lancamentos (COFONTE)",
            "CREATE INDEX idx_lanc_cocontacontabil ON lancamentos (COCONTACONTABIL)",
            "CREATE INDEX idx_lanc_coug ON lancamentos (COUG)",
            "CREATE INDEX idx_lanc_exercicio ON lancamentos (COEXERCICIO)",
            "CREATE INDEX idx_lanc_mes ON lancamentos (INMES)",
            "CREATE INDEX idx_lanc_exercicio_mes ON lancamentos (COEXERCICIO, INMES)"
        ]
        
        for idx_sql in indices:
            try:
                cursor.execute(idx_sql)
                print(f"    ✅ {idx_sql.split(' ON ')[0].replace('CREATE INDEX ', '')}")
            except Exception as e:
                print(f"    ⚠️  {idx_sql.split(' ON ')[0].replace('CREATE INDEX ', '')}: {e}")
        
        # Cria tabela de períodos
        print("\n  - Criando tabela de períodos...")
        cursor.execute("""
        CREATE TABLE dim_tempo AS
        SELECT DISTINCT 
            COEXERCICIO,
            INMES,
            COEXERCICIO || '-' || printf('%02d', INMES) as PERIODO,
            CASE INMES
                WHEN 1 THEN 'Janeiro'
                WHEN 2 THEN 'Fevereiro'
                WHEN 3 THEN 'Março'
                WHEN 4 THEN 'Abril'
                WHEN 5 THEN 'Maio'
                WHEN 6 THEN 'Junho'
                WHEN 7 THEN 'Julho'
                WHEN 8 THEN 'Agosto'
                WHEN 9 THEN 'Setembro'
                WHEN 10 THEN 'Outubro'
                WHEN 11 THEN 'Novembro'
                WHEN 12 THEN 'Dezembro'
            END as NOME_MES
        FROM lancamentos
        ORDER BY COEXERCICIO, INMES
        """)
        
        cursor.execute("CREATE INDEX idx_tempo_periodo ON dim_tempo (COEXERCICIO, INMES)")
        
        conn.commit()
        
        # Estatísticas
        print("\n--- Estatísticas ---")
        
        # Total por ano
        cursor.execute("""
        SELECT COEXERCICIO, COUNT(*) as total, SUM(VALANCAMENTO) as valor_total
        FROM lancamentos
        GROUP BY COEXERCICIO
        ORDER BY COEXERCICIO
        """)
        
        print("\n📊 Resumo por exercício:")
        for row in cursor.fetchall():
            print(f"   {row[0]}: {row[1]:,} lançamentos | R$ {row[2]:,.2f}")
        
        # Períodos disponíveis
        cursor.execute("SELECT COUNT(*) FROM dim_tempo")
        total_periodos = cursor.fetchone()[0]
        print(f"\n📅 Períodos disponíveis: {total_periodos}")
        
        conn.close()
        
        end_time = time.time()
        print(f"\n✅ Processamento concluído em {end_time - start_time:.2f} segundos!")
        print(f"💾 Banco criado em: {os.path.abspath(caminho_db)}")
        
    except Exception as e:
        print(f"\n❌ ERRO durante o processamento: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    processar_lancamentos()