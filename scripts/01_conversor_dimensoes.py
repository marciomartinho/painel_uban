# scripts/01_conversor_dimensoes.py (v4.1 - Híbrido Automático/Mapeado)
import pandas as pd
import sqlite3
import os
import time
import chardet
import glob
import json

# --- CONFIGURAÇÃO ---
if os.path.basename(os.getcwd()) == 'scripts':
    BASE_DIR = os.path.dirname(os.getcwd())
else:
    BASE_DIR = os.getcwd()

CAMINHO_DADOS_BRUTOS = os.path.join(BASE_DIR, 'dados', 'dados_brutos', 'dimensao')
CAMINHO_DB = os.path.join(BASE_DIR, 'dados', 'db')
NOME_BANCO_DADOS = 'banco_dimensoes.db'
ARQUIVO_CHAVES = os.path.join(CAMINHO_DB, 'chaves_primarias.json')

os.makedirs(CAMINHO_DB, exist_ok=True)

# --- MAPEAMENTO DE NOMES DE ARQUIVOS PARA TABELAS ---
# Garante que os nomes das tabelas principais sejam consistentes com o esperado pela aplicação
MAPEAMENTO_NOMES_CONHECIDOS = {
    'receita_categoria.csv': 'categorias',
    'receita_origem.csv': 'origens',
    'receita_especie.csv': 'especies',
    'receita_especificacao.csv': 'especificacoes',
    'receita_alinea.csv': 'alineas',
    'despesa_funcao.csv': 'funcoes',
    'despesa_subfuncao.csv': 'subfuncoes',
    'fonte.csv': 'fontes',
    'contacontabil.csv': 'contas',
    'unidadegestora.csv': 'unidades_gestoras'
}


# --- FUNÇÕES AUXILIARES (sem alteração) ---
def detectar_encoding(arquivo_path):
    with open(arquivo_path, 'rb') as file:
        return chardet.detect(file.read(100000))['encoding']

def ler_csv(arquivo_path):
    encodings = ['utf-8', 'utf-8-sig', 'latin1', 'iso-8859-1', 'cp1252']
    separadores = [';', ',']
    detected_encoding = detectar_encoding(arquivo_path)
    if detected_encoding: encodings.insert(0, detected_encoding)
    for encoding in encodings:
        for sep in separadores:
            try:
                df = pd.read_csv(arquivo_path, encoding=encoding, sep=sep, dtype=str, on_bad_lines='skip')
                if len(df.columns) > 1: return df
            except Exception: continue
    raise ValueError(f"Não foi possível ler o CSV: {arquivo_path}")

def ler_xlsx(arquivo_path):
    try:
        return pd.read_excel(arquivo_path, dtype=str)
    except Exception as e:
        raise ValueError(f"Não foi possível ler o XLSX: {arquivo_path}. Erro: {e}")

# --- FUNÇÃO PRINCIPAL ---
def criar_banco_dimensoes_automatico():
    print("=" * 60)
    print("CONVERSOR DE DIMENSÕES (v4.1 - Híbrido Automático/Mapeado)")
    print("=" * 60)
    
    start_time = time.time()
    caminho_db = os.path.join(CAMINHO_DB, NOME_BANCO_DADOS)

    chaves_salvas = {}
    if os.path.exists(ARQUIVO_CHAVES):
        with open(ARQUIVO_CHAVES, 'r', encoding='utf-8') as f:
            chaves_salvas = json.load(f)
        print(f"📖 Dicionário de chaves primárias carregado de '{ARQUIVO_CHAVES}'")

    # 1. Descoberta de arquivos e mapeamento de tabelas
    todos_arquivos = glob.glob(os.path.join(CAMINHO_DADOS_BRUTOS, '*.csv')) + \
                     glob.glob(os.path.join(CAMINHO_DADOS_BRUTOS, '*.xlsx'))
    if not todos_arquivos:
        print("Nenhum arquivo .csv ou .xlsx encontrado."); return

    mapeamento_tabelas = {}
    for f_path in todos_arquivos:
        nome_arquivo = os.path.basename(f_path)
        # Usa o nome mapeado se existir, senão gera um nome automático
        nome_tabela = MAPEAMENTO_NOMES_CONHECIDOS.get(nome_arquivo, os.path.splitext(nome_arquivo)[0].lower().replace('-', '_'))
        mapeamento_tabelas[nome_arquivo] = nome_tabela

    # 2. Comparação com o banco de dados
    tabelas_no_banco = set()
    if os.path.exists(caminho_db):
        conn_temp = sqlite3.connect(caminho_db)
        tabelas_no_banco = {row[0] for row in conn_temp.cursor().execute("SELECT name FROM sqlite_master WHERE type='table';")}
        conn_temp.close()

    novas_tabelas_nomes = set(mapeamento_tabelas.values()) - tabelas_no_banco
    novos_arquivos = [arquivo for arquivo, tabela in mapeamento_tabelas.items() if tabela in novas_tabelas_nomes]

    # 3. Menu de interação (sem alteração)
    escolha = ''
    arquivos_para_processar = list(mapeamento_tabelas.keys())
    if novas_tabelas_nomes:
        print("\nNovos arquivos/tabelas encontrados:")
        for arq in novos_arquivos: print(f"  - {arq} -> tabela '{mapeamento_tabelas[arq]}'")
        while escolha not in ['1', '2', '3']:
            escolha = input("\nO que deseja fazer?\n  [1] Processar APENAS os novos\n  [2] Re-processar TUDO (do zero)\n  [3] Cancelar\nEscolha: ")
    elif not os.path.exists(caminho_db):
        print("\nBanco de dados não existe. Iniciando processamento completo.")
        escolha = '2'
    else:
        print("\nNenhum arquivo novo. O banco parece atualizado.")
        while escolha not in ['1', '2']:
            escolha = input("\nO que deseja fazer?\n  [1] Forçar re-processamento de TUDO\n  [2] Cancelar\nEscolha: ")
            if escolha == '1': escolha = '2'
            elif escolha == '2': escolha = '3'
    
    if escolha == '3': print("Operação cancelada."); return
    elif escolha == '1':
        arquivos_para_processar = novos_arquivos
        print("\nOK, processando apenas os novos arquivos...")
    elif escolha == '2':
        print("\nOK, re-processando todos os arquivos do zero...")
        if os.path.exists(caminho_db): os.remove(caminho_db)
        if os.path.exists(ARQUIVO_CHAVES): os.remove(ARQUIVO_CHAVES)
        chaves_salvas = {}
        print("Banco de dados e arquivo de chaves antigos removidos.")

    # 4. Processamento (sem alteração na lógica interna)
    conn = sqlite3.connect(caminho_db)
    cursor = conn.cursor()
    cursor.executescript("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")
    
    print("\n--- Processando Tabelas ---")
    
    for arquivo in arquivos_para_processar:
        nome_tabela = mapeamento_tabelas[arquivo]
        print(f"\n📁 Processando '{arquivo}' para a tabela '{nome_tabela}'...")
        
        try:
            df = ler_csv(os.path.join(CAMINHO_DADOS_BRUTOS, arquivo)) if arquivo.lower().endswith('.csv') else ler_xlsx(os.path.join(CAMINHO_DADOS_BRUTOS, arquivo))
            df.columns = [col.lower() for col in df.columns]

            chave_primaria = chaves_salvas.get(nome_tabela)
            if not chave_primaria or chave_primaria not in df.columns:
                 while True:
                    print("\n  Abaixo estão as primeiras linhas e colunas disponíveis:")
                    print("  " + df.head(3).to_string().replace('\n', '\n  '))
                    pk_input = input(f"  Qual o nome da coluna de chave primária para '{nome_tabela}'? (Deixe em branco para não criar): ").strip().lower()
                    
                    if not pk_input:
                        print("  Nenhum índice será criado para esta tabela.")
                        chaves_salvas.pop(nome_tabela, None)
                        chave_primaria = None
                        break
                    elif pk_input in df.columns:
                        chave_primaria = pk_input
                        chaves_salvas[nome_tabela] = chave_primaria
                        print(f"  OK, índice será criado em '{chave_primaria}'.")
                        break
                    else:
                        print(f"  ❌ Erro: A coluna '{pk_input}' não existe. Tente novamente.")

            df.to_sql(nome_tabela, conn, if_exists='replace', index=False)
            if chave_primaria:
                cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{nome_tabela}_{chave_primaria} ON {nome_tabela} ({chave_primaria})")
            
            print(f"   ✅ Tabela '{nome_tabela}' criada com {len(df):,} registros.")
        except Exception as e:
            print(f"   ❌ Erro ao processar o arquivo '{arquivo}': {e}")
            
    with open(ARQUIVO_CHAVES, 'w', encoding='utf-8') as f:
        json.dump(chaves_salvas, f, indent=4, ensure_ascii=False)
    print(f"\n💾 Dicionário de chaves primárias salvo em '{ARQUIVO_CHAVES}'")
    
    print("\n🔧 Otimizando banco de dados...")
    cursor.executescript("ANALYZE; VACUUM;")
    conn.commit()
    conn.close()
    
    print("\n" + "=" * 60)
    print(f"🎉 Processamento Concluído em {time.time() - start_time:.2f}s!")
    print(f"💾 Banco de dados salvo em: {os.path.abspath(caminho_db)}")

if __name__ == "__main__":
    criar_banco_dimensoes_automatico()