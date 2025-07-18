# povoar_postgres.py
import os
import pandas as pd
from sqlalchemy import create_engine, text
import time
import chardet

# --- CONFIGURAÇÃO ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CAMINHO_DADOS_BRUTOS = os.path.join(BASE_DIR, 'dados', 'dados_brutos')

ARQUIVOS_PARA_POVOAR = {
    # Dimensões de Receita e Gerais
    'dimensao/receita_categoria.csv': ('dimensoes', 'categorias'),
    'dimensao/receita_origem.csv': ('dimensoes', 'origens'),
    'dimensao/receita_especie.csv': ('dimensoes', 'especies'),
    'dimensao/receita_especificacao.csv': ('dimensoes', 'especificacoes'),
    'dimensao/receita_alinea.csv': ('dimensoes', 'alineas'),
    'dimensao/fonte.xlsx': ('dimensoes', 'fontes'),
    'dimensao/contacontabil.xlsx': ('dimensoes', 'contas'),
    'dimensao/unidadegestora.csv': ('dimensoes', 'unidades_gestoras'),
    'dimensao/elemento.csv': ('dimensoes', 'elemento'),
    'dimensao/gestao.csv': ('dimensoes', 'gestao'),
    # Dimensões de Despesa
    'dimensao/despesa_grupo.xlsx': ('dimensoes', 'despesa_grupo'),
    'dimensao/despesa_categoria.xlsx': ('dimensoes', 'despesa_categoria'),
    'dimensao/despesa_modalidade.xlsx': ('dimensoes', 'despesa_modalidade'),
    'dimensao/classificacaoorcamentaria.xlsx': ('dimensoes', 'classificacao_orcamentaria'),
    # Tabelas Fato de Receita
    'ReceitaSaldo.xlsx': ('public', 'fato_saldos'),
    'ReceitaLancamento.xlsx': ('public', 'lancamentos'),
    # Tabelas Fato de Despesa
    'DespesaSaldo.xlsx': ('public', 'fato_saldo_despesa'),
    'DespesaLancamento.xlsx': ('public', 'fato_lancamento_despesa')
}

# Colunas a serem lidas dos arquivos de despesa
COLUNAS_SALDO_DESPESA = [
    'COEXERCICIO', 'COUG', 'COGESTAO', 'COCONTACONTABIL', 'COCONTACORRENTE', 'INMES',
    'INESFERA', 'COUO', 'COFUNCAO', 'COSUBFUNCAO', 'COPROGRAMA', 'COPROJETO',
    'COSUBTITULO', 'COFONTE', 'CONATUREZA', 'INCATEGORIA', 'VACREDITO', 'VADEBITO', 'INTIPOADM'
]
COLUNAS_LANCAMENTO_DESPESA = [
    'COEXERCICIO', 'COUG', 'COGESTAO', 'NUDOCUMENTO', 'COEVENTO', 'COCONTACONTABIL', 'COCONTACORRENTE', 
    'INMES', 'DALANCAMENTO', 'VALANCAMENTO', 'INDEBITOCREDITO', 'INABREENCERRA', 'COUGDESTINO', 
    'COGESTAODESTINO', 'DATRANSACAO', 'COUGCONTAB', 'COGESTAOCONTAB'
]

def detectar_encoding(arquivo_path):
    with open(arquivo_path, 'rb') as file:
        return chardet.detect(file.read(100000))['encoding']

def ler_arquivo(caminho_completo, nome_tabela):
    if not os.path.exists(caminho_completo):
        print(f"  ⚠️  AVISO: Arquivo não encontrado, pulando: {os.path.basename(caminho_completo)}")
        return None
    print(f"  Lendo arquivo: {os.path.basename(caminho_completo)}...")
    
    colunas_para_ler = None
    if nome_tabela == 'fato_saldo_despesa':
        colunas_para_ler = COLUNAS_SALDO_DESPESA
    elif nome_tabela == 'fato_lancamento_despesa':
        colunas_para_ler = COLUNAS_LANCAMENTO_DESPESA

    if caminho_completo.endswith('.xlsx'):
        try:
            df = pd.read_excel(caminho_completo, usecols=lambda c: c in colunas_para_ler if colunas_para_ler else True, dtype=str)
            return df
        except Exception as e:
            print(f"  ❌ Erro ao ler XLSX {os.path.basename(caminho_completo)}: {e}")
            return None

    encodings = ['utf-8', 'latin1', 'iso-8859-1', 'cp1252', 'utf-8-sig']
    separadores = [';', ',']
    detected = detectar_encoding(caminho_completo)
    if detected and detected not in encodings: encodings.insert(0, detected)
    for encoding in encodings:
        for sep in separadores:
            try:
                df = pd.read_csv(caminho_completo, encoding=encoding, sep=sep, dtype=str, on_bad_lines='warn')
                if len(df.columns) > 1:
                    print(f"   -> Sucesso com Encoding: {encoding}, Separador: '{sep}'")
                    return df
            except Exception:
                continue
    print(f"  ❌ Erro: Não foi possível ler ou parsear o arquivo CSV: {os.path.basename(caminho_completo)}. Pulando.")
    return None

def processar_dataframe(df, nome_tabela):
    df.columns = [str(col).lower().replace(' ', '_') for col in df.columns]

    if nome_tabela in ['fato_saldos', 'lancamentos']:
        print(f"  Processando colunas de receita para '{nome_tabela}'...")
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

    elif nome_tabela == 'fato_saldo_despesa':
        print("  Processando colunas para 'fato_saldo_despesa'...")
        def extrair_classe_orcamentaria(cocontacorrente):
            s = str(cocontacorrente).strip()
            return s[32:40] if len(s) == 40 else None
        df['coclasseorc'] = df['cocontacorrente'].apply(extrair_classe_orcamentaria)

    elif nome_tabela == 'fato_lancamento_despesa':
        print("  Processando colunas para 'fato_lancamento_despesa'...")
        def extrair_campos_despesa(row):
            s = str(row['cocontacorrente']).strip()
            campos = ['inesfera', 'couo', 'cofuncao', 'cosubfuncao', 'coprograma', 'coprojeto', 
                      'cosubtitulo', 'cofonte', 'conatureza', 'incategoria', 'cogrupo', 
                      'comodalidade', 'coelemento', 'subelemento', 'coclasseorc']
            for campo in campos: row[campo] = None # Inicializa todos como nulo
            
            if len(s) >= 38:
                row['inesfera'] = s[0]
                row['couo'] = s[1:6]
                row['cofuncao'] = s[6:8]
                row['cosubfuncao'] = s[8:11]
                row['coprograma'] = s[11:15]
                row['coprojeto'] = s[15:19]
                row['cosubtitulo'] = s[19:23]
                row['cofonte'] = s[23:32]
                row['conatureza'] = s[32:38]
                row['incategoria'] = s[32]
                row['cogrupo'] = s[33]
                row['comodalidade'] = s[34:36]
                row['coelemento'] = s[36:38]
                if len(s) == 40:
                    row['subelemento'] = s[38:40]
                    row['coclasseorc'] = s[32:40]
            return row
        df = df.apply(extrair_campos_despesa, axis=1)
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
        df = ler_arquivo(os.path.join(CAMINHO_DADOS_BRUTOS, arquivo), nome_tabela)
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