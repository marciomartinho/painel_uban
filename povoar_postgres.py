# povoar_postgres.py
import os
import pandas as pd
from sqlalchemy import create_engine, text
import time
import chardet

# --- CONFIGURAÇÃO ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CAMINHO_DADOS_BRUTOS = os.path.join(BASE_DIR, 'dados', 'dados_brutos')

# Mapeamento de arquivos para tabelas, indicando o esquema
ARQUIVOS_PARA_POVOAR = {
    # Tabelas no esquema 'dimensoes'
    'dimensao/receita_categoria.csv': ('dimensoes', 'categorias'),
    'dimensao/receita_origem.csv': ('dimensoes', 'origens'),
    'dimensao/receita_especie.csv': ('dimensoes', 'especies'),
    'dimensao/receita_especificacao.csv': ('dimensoes', 'especificacoes'),
    'dimensao/receita_alinea.csv': ('dimensoes', 'alineas'),
    'dimensao/fonte.csv': ('dimensoes', 'fontes'),
    'dimensao/contacontabil.csv': ('dimensoes', 'contas'),
    'dimensao/unidadegestora.csv': ('dimensoes', 'unidades_gestoras'),
    'dimensao/elemento.csv': ('dimensoes', 'elemento'),
    'dimensao/gestao.csv': ('dimensoes', 'gestao'),
    # Tabelas no esquema 'public' (principal)
    'ReceitaSaldo.xlsx': ('public', 'fato_saldos'),
    'ReceitaLancamento.xlsx': ('public', 'lancamentos')
}

def detectar_encoding(arquivo_path):
    """Detecta o encoding de um arquivo CSV."""
    with open(arquivo_path, 'rb') as file:
        return chardet.detect(file.read(100000))['encoding']

def ler_arquivo(caminho_completo):
    """Lê um arquivo .csv ou .xlsx e retorna um DataFrame."""
    if not os.path.exists(caminho_completo):
        print(f"  ⚠️  AVISO: Arquivo não encontrado, pulando: {os.path.basename(caminho_completo)}")
        return None

    print(f"  Lendo arquivo: {os.path.basename(caminho_completo)}...")
    if caminho_completo.endswith('.xlsx'):
        return pd.read_excel(caminho_completo, engine='openpyxl')
    
    # --- LÓGICA DE LEITURA DE CSV APRIMORADA ---
    encodings_para_tentar = ['utf-8', 'latin1', 'iso-8859-1', 'cp1252', 'utf-8-sig']
    separadores = [';', ',']
    
    detected_encoding = detectar_encoding(caminho_completo)
    if detected_encoding and detected_encoding not in encodings_para_tentar:
        encodings_para_tentar.insert(0, detected_encoding)

    for encoding in encodings_para_tentar:
        for sep in separadores:
            try:
                # Tenta ler o arquivo com as configurações atuais
                df = pd.read_csv(caminho_completo, encoding=encoding, sep=sep, dtype=str, on_bad_lines='warn')
                # Se o separador funcionou (gerou mais de uma coluna), retorna o dataframe
                if len(df.columns) > 1:
                    print(f"   -> Sucesso com Encoding: {encoding}, Separador: '{sep}'")
                    return df
            except Exception:
                # Se der erro, simplesmente continua para a próxima tentativa
                continue
    
    print(f"  ❌ Erro: Não foi possível ler ou parsear o arquivo CSV: {os.path.basename(caminho_completo)}. Pulando.")
    return None
    # --- FIM DA LÓGICA APRIMORADA ---

def processar_dataframe(df, nome_tabela):
    """Aplica transformações nos DataFrames."""
    df.columns = [str(col).lower().replace(' ', '_') for col in df.columns]

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
                if conta.startswith('5'): return row.get('vadebito', 0) - row.get('vacredito', 0)
                if conta.startswith('6'): return row.get('vacredito', 0) - row.get('vadebito', 0)
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
        print("\n❌ ERRO: Variável de ambiente 'DATABASE_URL' não definida.")
        return

    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql+psycopg2://", 1)

    try:
        engine = create_engine(db_url)
        with engine.connect() as connection:
            print("✅ Conexão com PostgreSQL estabelecida!")
            connection.execute(text("CREATE SCHEMA IF NOT EXISTS dimensoes"))
            connection.commit()
            print("✅ Esquema 'dimensoes' garantido.")
    except Exception as e:
        print(f"\n❌ ERRO: Não foi possível conectar ou criar esquema: {e}")
        return

    start_time_total = time.time()

    for arquivo, (esquema, nome_tabela) in ARQUIVOS_PARA_POVOAR.items():
        print(f"\n--- Processando: {arquivo} -> Esquema: {esquema}, Tabela: {nome_tabela} ---")
        start_time_tabela = time.time()
        
        df = ler_arquivo(os.path.join(CAMINHO_DADOS_BRUTOS, arquivo))
        if df is None: continue

        try:
            df = processar_dataframe(df, nome_tabela)
            
            print(f"  Enviando {len(df)} registros para a tabela '{esquema}.{nome_tabela}'...")
            df.to_sql(name=nome_tabela, con=engine, schema=esquema, if_exists='replace', index=False, chunksize=10000)
            
            end_time_tabela = time.time()
            print(f"  ✅ Tabela populada com sucesso em {end_time_tabela - start_time_tabela:.2f} segundos.")
        except Exception as e:
            print(f"  ❌ ERRO ao processar o arquivo {arquivo}: {e}")

    try:
        print("\n--- Criando tabela 'dim_tempo' a partir de 'fato_saldos' ---")
        with engine.connect() as connection:
            connection.execute(text("DROP TABLE IF EXISTS public.dim_tempo;"))
            query = text("""
            CREATE TABLE public.dim_tempo AS
            SELECT DISTINCT coexercicio, inmes,
                CASE inmes
                    WHEN 1 THEN 'Janeiro' WHEN 2 THEN 'Fevereiro' WHEN 3 THEN 'Março'
                    WHEN 4 THEN 'Abril' WHEN 5 THEN 'Maio' WHEN 6 THEN 'Junho'
                    WHEN 7 THEN 'Julho' WHEN 8 THEN 'Agosto' WHEN 9 THEN 'Setembro'
                    WHEN 10 THEN 'Outubro' WHEN 11 THEN 'Novembro' WHEN 12 THEN 'Dezembro'
                END as nome_mes
            FROM public.fato_saldos ORDER BY coexercicio, inmes;
            """)
            connection.execute(query)
            connection.commit()
            print("  ✅ Tabela 'public.dim_tempo' criada com sucesso.")
    except Exception as e:
        print(f"  ❌ ERRO ao criar 'dim_tempo': {e}")

    end_time_total = time.time()
    print("\n" + "="*60)
    print(f"🎉 POVOAMENTO DO BANCO DE DADOS CONCLUÍDO!")
    print(f"⏱️  Tempo total de execução: {end_time_total - start_time_total:.2f} segundos.")
    print("="*60)

if __name__ == '__main__':
    main()