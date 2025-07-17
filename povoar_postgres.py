# povoar_postgres.py
import os
import pandas as pd
from sqlalchemy import create_engine
import time
import chardet

# --- CONFIGURAÇÃO ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CAMINHO_DADOS_BRUTOS = os.path.join(BASE_DIR, 'dados', 'dados_brutos')

# Mapeamento de arquivos para nomes de tabelas (tudo em minúsculas)
ARQUIVOS_PARA_POVOAR = {
    'dimensao/receita_categoria.csv': 'categorias',
    'dimensao/receita_origem.csv': 'origens',
    'dimensao/receita_especie.csv': 'especies',
    'dimensao/receita_especificacao.csv': 'especificacoes',
    'dimensao/receita_alinea.csv': 'alineas',
    'dimensao/fonte.csv': 'fontes',
    'dimensao/contacontabil.csv': 'contas',
    'dimensao/unidadegestora.csv': 'unidades_gestoras',
    'ReceitaSaldo.xlsx': 'fato_saldos',
    'ReceitaLancamento.xlsx': 'lancamentos'
}

def detectar_encoding(arquivo_path):
    """Detecta o encoding de um arquivo CSV."""
    with open(arquivo_path, 'rb') as file:
        raw_data = file.read(100000)
        result = chardet.detect(raw_data)
        return result['encoding']

def ler_arquivo(caminho_completo):
    """Lê um arquivo .csv ou .xlsx e retorna um DataFrame."""
    if caminho_completo.endswith('.xlsx'):
        print(f"  Lendo Excel: {os.path.basename(caminho_completo)}...")
        return pd.read_excel(caminho_completo, engine='openpyxl')
    elif caminho_completo.endswith('.csv'):
        print(f"  Lendo CSV: {os.path.basename(caminho_completo)}...")
        encoding = detectar_encoding(caminho_completo)
        print(f"    - Encoding detectado: {encoding}")
        return pd.read_csv(caminho_completo, encoding=encoding, on_bad_lines='skip', sep=';')
    return None

def processar_dataframe_especial(df, nome_tabela):
    """Aplica transformações especiais para as tabelas de fatos."""
    # Primeiro, padroniza TODOS os nomes de colunas para minúsculas
    df.columns = [str(col).lower() for col in df.columns]
    print("    - Nomes de colunas padronizados para minúsculas.")

    if nome_tabela in ['fato_saldos', 'lancamentos']:
        print(f"  Processando colunas especiais para '{nome_tabela}'...")
        if 'cocontacorrente' in df.columns:
            df['cocontacorrente'] = df['cocontacorrente'].astype(str).str.strip()
            df['categoriareceita'] = df['cocontacorrente'].str[0:1]
            df['cofontereceita'] = df['cocontacorrente'].str[0:2]
            df['cosubfontereceita'] = df['cocontacorrente'].str[0:3]
            df['corubrica'] = df['cocontacorrente'].str[0:4]
            df['coalinea'] = df['cocontacorrente'].str[0:6]
            df['cofonte'] = df['cocontacorrente'].str[8:17]

        if nome_tabela == 'fato_saldos':
            df['vadebito'] = pd.to_numeric(df['vadebito'], errors='coerce').fillna(0)
            df['vacredito'] = pd.to_numeric(df['vacredito'], errors='coerce').fillna(0)
            
            def calcular_saldo(row):
                conta = str(row.get('cocontacontabil', '')).strip()
                debito = row.get('vadebito', 0)
                credito = row.get('vacredito', 0)
                if conta.startswith('5'): return debito - credito
                if conta.startswith('6'): return credito - debito
                return 0.0
            
            df['saldo_contabil'] = df.apply(calcular_saldo, axis=1)
            print("    - Coluna 'saldo_contabil' calculada.")
    return df

def main():
    print("="*60)
    print("INICIANDO SCRIPT DE POVOAMENTO DO BANCO DE DADOS POSTGRESQL")
    print("="*60)

    db_url = os.environ.get('DATABASE_URL')
    if not db_url:
        print("\n❌ ERRO: A variável de ambiente 'DATABASE_URL' não foi definida.")
        print("   Defina-a com a URL pública do seu banco de dados do Railway.")
        return

    # IMPORTANTE: Para o SQLAlchemy se conectar corretamente ao psycopg2
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)

    print(f"\nConectando ao banco: ...{db_url[-20:]}")
    
    try:
        engine = create_engine(db_url)
        with engine.connect() as connection:
             print("✅ Conexão com PostgreSQL estabelecida com sucesso!")
    except Exception as e:
        print(f"\n❌ ERRO: Não foi possível conectar ao banco de dados: {e}")
        return

    start_time_total = time.time()

    for arquivo, nome_tabela in ARQUIVOS_PARA_POVOAR.items():
        print(f"\n--- Processando: {arquivo} -> Tabela: {nome_tabela} ---")
        start_time_tabela = time.time()
        caminho_completo = os.path.join(CAMINHO_DADOS_BRUTOS, arquivo)

        if not os.path.exists(caminho_completo):
            print(f"  ⚠️  AVISO: Arquivo não encontrado, pulando: {caminho_completo}")
            continue

        try:
            df = ler_arquivo(caminho_completo)
            if df is None: continue
            
            df = processar_dataframe_especial(df, nome_tabela)
            
            print(f"  Enviando {len(df)} registros para a tabela '{nome_tabela}'...")
            df.to_sql(name=nome_tabela, con=engine, if_exists='replace', index=False, chunksize=10000)
            
            end_time_tabela = time.time()
            print(f"  ✅ Tabela '{nome_tabela}' populada com sucesso em {end_time_tabela - start_time_tabela:.2f} segundos.")

        except Exception as e:
            print(f"  ❌ ERRO ao processar o arquivo {arquivo}: {e}")
            import traceback
            traceback.print_exc()

    try:
        print("\n--- Criando tabela 'dim_tempo' a partir de 'fato_saldos' ---")
        with engine.connect() as connection:
            trans = connection.begin()
            connection.execute("DROP TABLE IF EXISTS dim_tempo;")
            query = """
            CREATE TABLE dim_tempo AS
            SELECT DISTINCT coexercicio, inmes,
                CASE inmes
                    WHEN 1 THEN 'Janeiro' WHEN 2 THEN 'Fevereiro' WHEN 3 THEN 'Março'
                    WHEN 4 THEN 'Abril' WHEN 5 THEN 'Maio' WHEN 6 THEN 'Junho'
                    WHEN 7 THEN 'Julho' WHEN 8 THEN 'Agosto' WHEN 9 THEN 'Setembro'
                    WHEN 10 THEN 'Outubro' WHEN 11 THEN 'Novembro' WHEN 12 THEN 'Dezembro'
                END as nome_mes
            FROM fato_saldos ORDER BY coexercicio, inmes;
            """
            connection.execute(query)
            trans.commit()
            print("  ✅ Tabela 'dim_tempo' criada com sucesso.")
    except Exception as e:
        print(f"  ❌ ERRO ao criar 'dim_tempo': {e}")

    end_time_total = time.time()
    print("\n" + "="*60)
    print(f"🎉 POVOAMENTO DO BANCO DE DADOS CONCLUÍDO!")
    print(f"⏱️  Tempo total de execução: {end_time_total - start_time_total:.2f} segundos.")
    print("="*60)

if __name__ == '__main__':
    try:
        import sqlalchemy
    except ImportError:
        print("Instalando biblioteca SQLAlchemy necessária...")
        import subprocess
        subprocess.check_call(['pip', 'install', 'sqlalchemy'])
    main()
