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
    """
    Calcula o saldo contábil baseado na regra:
    - Contas que começam com 5: saldo = débito - crédito
    - Contas que começam com 6: saldo = crédito - débito
    
    Args:
        conta_contabil: Código da conta contábil
        debito: Valor do débito
        credito: Valor do crédito
    
    Returns:
        float: Saldo contábil calculado
    """
    conta_str = str(conta_contabil).strip()
    
    # Garante que os valores são numéricos
    debito = float(debito) if debito else 0.0
    credito = float(credito) if credito else 0.0
    
    if conta_str.startswith('5'):
        return debito - credito
    elif conta_str.startswith('6'):
        return credito - debito
    else:
        # Para outras contas (não deveria existir no contexto de receitas)
        return 0.0

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

def processar_saldos():
    """Processa o arquivo de saldos e cria o banco de dados"""
    
    print("=" * 60)
    print("CONVERSOR DE SALDOS DE RECEITA")
    print("=" * 60)
    
    start_time = time.time()
    
    # Arquivos
    arquivo_excel = os.path.join(CAMINHO_DADOS_BRUTOS, 'ReceitaSaldo.xlsx')
    caminho_db = os.path.join(CAMINHO_DB, 'banco_saldo_receita.db')
    
    print(f"\nArquivo fonte: {arquivo_excel}")
    print(f"Banco destino: {caminho_db}")
    
    # Verifica se o arquivo existe
    if not os.path.exists(arquivo_excel):
        print(f"\n❌ ERRO: Arquivo '{arquivo_excel}' não encontrado!")
        return
    
    # Verifica se o banco já existe
    if os.path.exists(caminho_db):
        resposta = input("\nBanco de saldos já existe. Deseja substituí-lo? (s/n): ")
        if resposta.lower() != 's':
            print("Operação cancelada.")
            return
        os.remove(caminho_db)
        print("Banco antigo removido.")
    
    print("\n--- Processando Saldos ---")
    
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
        
        # Converte colunas numéricas
        print("\n  - Convertendo colunas numéricas...")
        df['VADEBITO'] = pd.to_numeric(df['VADEBITO'], errors='coerce').fillna(0)
        df['VACREDITO'] = pd.to_numeric(df['VACREDITO'], errors='coerce').fillna(0)
        print("    ✅ VADEBITO e VACREDITO convertidos")
        
        # CALCULA O SALDO CONTÁBIL
        print("\n  - Calculando saldo contábil...")
        df['saldo_contabil'] = df.apply(
            lambda row: calcular_saldo_contabil(
                row['COCONTACONTABIL'], 
                row['VADEBITO'], 
                row['VACREDITO']
            ), 
            axis=1
        )
        
        # Estatísticas do cálculo
        contas_5 = df[df['COCONTACONTABIL'].astype(str).str.startswith('5')]
        contas_6 = df[df['COCONTACONTABIL'].astype(str).str.startswith('6')]
        
        print(f"    ✅ Saldo calculado para {len(df):,} registros")
        print(f"       - Contas tipo 5 (débito - crédito): {len(contas_5):,} registros")
        print(f"       - Contas tipo 6 (crédito - débito): {len(contas_6):,} registros")
        
        # Exemplos de cálculo
        print("\n    Exemplos de cálculo:")
        for tipo, contas_tipo in [('5', contas_5), ('6', contas_6)]:
            if len(contas_tipo) > 0:
                exemplo = contas_tipo.iloc[0]
                print(f"      Conta {exemplo['COCONTACONTABIL']} (tipo {tipo}):")
                print(f"        Débito: {exemplo['VADEBITO']:,.2f}")
                print(f"        Crédito: {exemplo['VACREDITO']:,.2f}")
                print(f"        Saldo: {exemplo['saldo_contabil']:,.2f}")
        
        # Conecta ao banco
        conn = sqlite3.connect(caminho_db)
        
        # Salva no banco
        print("\n  - Salvando no banco de dados...")
        df.to_sql('fato_saldos', conn, if_exists='replace', index=False, chunksize=10000)
        
        # Cria índices
        print("\n  - Criando índices...")
        cursor = conn.cursor()
        
        indices = [
            "CREATE INDEX idx_saldo_categoriareceita ON fato_saldos (CATEGORIARECEITA)",
            "CREATE INDEX idx_saldo_cofontereceita ON fato_saldos (COFONTERECEITA)",
            "CREATE INDEX idx_saldo_cosubfontereceita ON fato_saldos (COSUBFONTERECEITA)",
            "CREATE INDEX idx_saldo_corubrica ON fato_saldos (CORUBRICA)",
            "CREATE INDEX idx_saldo_coalinea ON fato_saldos (COALINEA)",
            "CREATE INDEX idx_saldo_cofonte ON fato_saldos (COFONTE)",
            "CREATE INDEX idx_saldo_cocontacontabil ON fato_saldos (COCONTACONTABIL)",
            "CREATE INDEX idx_saldo_coug ON fato_saldos (COUG)",
            "CREATE INDEX idx_saldo_exercicio ON fato_saldos (COEXERCICIO)",
            "CREATE INDEX idx_saldo_mes ON fato_saldos (INMES)",
            "CREATE INDEX idx_saldo_exercicio_mes ON fato_saldos (COEXERCICIO, INMES)",
            "CREATE INDEX idx_saldo_contabil ON fato_saldos (saldo_contabil)"  # Novo índice
        ]
        
        for idx_sql in indices:
            try:
                cursor.execute(idx_sql)
                print(f"    ✅ {idx_sql.split(' ON ')[0].replace('CREATE INDEX ', '')}")
            except Exception as e:
                print(f"   ⚠️  {idx_sql.split(' ON ')[0].replace('CREATE INDEX ', '')}: {e}")
        
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
        FROM fato_saldos
        ORDER BY COEXERCICIO, INMES
        """)
        
        cursor.execute("CREATE INDEX idx_tempo_periodo ON dim_tempo (COEXERCICIO, INMES)")
        
        conn.commit()
        
        # Estatísticas
        print("\n--- Estatísticas ---")
        
        # Total por ano com o novo saldo
        cursor.execute("""
        SELECT 
            COEXERCICIO, 
            COUNT(*) as total_registros,
            SUM(VADEBITO) as total_debito,
            SUM(VACREDITO) as total_credito,
            SUM(saldo_contabil) as total_saldo
        FROM fato_saldos
        GROUP BY COEXERCICIO
        ORDER BY COEXERCICIO
        """)
        
        print("\n📊 Resumo por exercício:")
        print(f"{'Ano':<6} | {'Registros':>10} | {'Total Débito':>20} | {'Total Crédito':>20} | {'Saldo Total':>20}")
        print("-" * 80)
        for row in cursor.fetchall():
            print(f"{row[0]:<6} | {row[1]:>10,} | R$ {row[2]:>17,.2f} | R$ {row[3]:>17,.2f} | R$ {row[4]:>17,.2f}")
        
        # Estatísticas por tipo de conta
        cursor.execute("""
        SELECT 
            SUBSTR(COCONTACONTABIL, 1, 1) as tipo_conta,
            COUNT(*) as total_registros,
            SUM(VADEBITO) as total_debito,
            SUM(VACREDITO) as total_credito,
            SUM(saldo_contabil) as total_saldo,
            SUM(CASE WHEN saldo_contabil > 0 THEN 1 ELSE 0 END) as saldos_positivos,
            SUM(CASE WHEN saldo_contabil < 0 THEN 1 ELSE 0 END) as saldos_negativos,
            SUM(CASE WHEN saldo_contabil = 0 THEN 1 ELSE 0 END) as saldos_zero
        FROM fato_saldos
        WHERE COEXERCICIO = (SELECT MAX(COEXERCICIO) FROM fato_saldos)
        GROUP BY SUBSTR(COCONTACONTABIL, 1, 1)
        ORDER BY tipo_conta
        """)
        
        print(f"\n📈 Análise por tipo de conta (último exercício):")
        print(f"{'Tipo':<4} | {'Registros':>10} | {'Saldo Total':>20} | {'Positivos':>10} | {'Negativos':>10} | {'Zeros':>10}")
        print("-" * 70)
        for row in cursor.fetchall():
            print(f"{row[0]:<4} | {row[1]:>10,} | R$ {row[4]:>17,.2f} | {row[5]:>10,} | {row[6]:>10,} | {row[7]:>10,}")
        
        # Validação da regra
        print("\n🔍 Validação da regra de cálculo (5 exemplos de cada tipo):")
        
        # Exemplos de contas tipo 5
        cursor.execute("""
        SELECT COCONTACONTABIL, VADEBITO, VACREDITO, saldo_contabil
        FROM fato_saldos
        WHERE COCONTACONTABIL LIKE '5%' AND (VADEBITO != 0 OR VACREDITO != 0)
        LIMIT 5
        """)
        
        print("\n  Contas tipo 5 (saldo = débito - crédito):")
        for row in cursor.fetchall():
            calc = row[1] - row[2]
            status = "✅" if abs(calc - row[3]) < 0.01 else "❌"
            print(f"    {status} Conta {row[0]}: {row[1]:,.2f} - {row[2]:,.2f} = {row[3]:,.2f}")
        
        # Exemplos de contas tipo 6
        cursor.execute("""
        SELECT COCONTACONTABIL, VADEBITO, VACREDITO, saldo_contabil
        FROM fato_saldos
        WHERE COCONTACONTABIL LIKE '6%' AND (VADEBITO != 0 OR VACREDITO != 0)
        LIMIT 5
        """)
        
        print("\n  Contas tipo 6 (saldo = crédito - débito):")
        for row in cursor.fetchall():
            calc = row[2] - row[1]
            status = "✅" if abs(calc - row[3]) < 0.01 else "❌"
            print(f"    {status} Conta {row[0]}: {row[2]:,.2f} - {row[1]:,.2f} = {row[3]:,.2f}")
        
        # Períodos disponíveis
        cursor.execute("SELECT COUNT(*) FROM dim_tempo")
        total_periodos = cursor.fetchone()[0]
        print(f"\n📅 Períodos disponíveis: {total_periodos}")
        
        conn.close()
        
        end_time = time.time()
        print(f"\n✅ Processamento concluído em {end_time - start_time:.2f} segundos!")
        print(f"💾 Banco criado em: {os.path.abspath(caminho_db)}")
        print(f"📊 Coluna 'saldo_contabil' adicionada com sucesso!")
        
    except Exception as e:
        print(f"\n❌ ERRO durante o processamento: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    processar_saldos()