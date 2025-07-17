import pandas as pd
import sqlite3
import os
import time
import chardet

# --- CONFIGURAÇÃO ---
if os.path.basename(os.getcwd()) == 'scripts':
    BASE_DIR = os.path.dirname(os.getcwd())
else:
    BASE_DIR = os.getcwd()

# Caminhos
CAMINHO_DADOS_BRUTOS = os.path.join(BASE_DIR, 'dados', 'dados_brutos', 'dimensao')
CAMINHO_DB = os.path.join(BASE_DIR, 'dados', 'db')

# Criar pasta db se não existir
os.makedirs(CAMINHO_DB, exist_ok=True)

# <<< CORREÇÃO: Chaves primárias também em minúsculas para consistência >>>
TABELAS_DIMENSAO = {
    'categorias': {
        'arquivo': 'receita_categoria.csv',
        'chave_primaria': 'cocategoriareceita',
        'descricao': 'Categorias de Receita'
    },
    'origens': {
        'arquivo': 'receita_origem.csv',
        'chave_primaria': 'cofontereceita',
        'descricao': 'Origens de Receita'
    },
    'especies': {
        'arquivo': 'receita_especie.csv',
        'chave_primaria': 'cosubfontereceita',
        'descricao': 'Espécies de Receita'
    },
    'especificacoes': {
        'arquivo': 'receita_especificacao.csv',
        'chave_primaria': 'corubrica',
        'descricao': 'Especificações/Rubricas'
    },
    'alineas': {
        'arquivo': 'receita_alinea.csv',
        'chave_primaria': 'coalinea',
        'descricao': 'Alíneas'
    },
    'fontes': {
        'arquivo': 'fonte.csv',
        'chave_primaria': 'cofonte',
        'descricao': 'Fontes de Recursos'
    },
    'contas': {
        'arquivo': 'contacontabil.csv',
        'chave_primaria': 'cocontacontabil',
        'descricao': 'Contas Contábeis'
    },
    'unidades_gestoras': {
        'arquivo': 'unidadegestora.csv',
        'chave_primaria': 'coug',
        'descricao': 'Unidades Gestoras'
    }
}

def detectar_encoding(arquivo_path):
    """Detecta automaticamente o encoding do arquivo"""
    with open(arquivo_path, 'rb') as file:
        raw_data = file.read(100000)
        result = chardet.detect(raw_data)
        return result['encoding']

def ler_csv_com_encoding_correto(arquivo_csv):
    """Tenta ler o CSV com diferentes encodings e separadores."""
    encodings = ['utf-8', 'utf-8-sig', 'latin1', 'iso-8859-1', 'cp1252']
    separadores = [';', ',']
    
    detected_encoding = detectar_encoding(arquivo_csv)
    if detected_encoding:
        encodings.insert(0, detected_encoding)
    
    for encoding in encodings:
        for sep in separadores:
            try:
                df = pd.read_csv(arquivo_csv, encoding=encoding, on_bad_lines='skip', sep=sep, dtype=str)
                # Verifica se o separador funcionou (mais de uma coluna)
                if len(df.columns) > 1:
                    print(f"   📝 Encoding: {encoding}, Separador: '{sep}'")
                    return df
            except Exception:
                continue
    
    raise ValueError(f"Não foi possível ler ou parsear o arquivo {arquivo_csv}")

def criar_banco_dimensoes():
    """Cria o banco de dados com todas as tabelas de dimensão"""
    
    print("=" * 60)
    print("CONVERSOR DE TABELAS DIMENSÃO (PADRONIZADO)")
    print("=" * 60)
    
    start_time = time.time()
    
    caminho_db = os.path.join(CAMINHO_DB, 'banco_dimensoes.db')
    
    if os.path.exists(caminho_db):
        resposta = input("\nBanco de dimensões já existe. Deseja substituí-lo? (s/n): ")
        if resposta.lower() != 's':
            print("Operação cancelada.")
            return
        os.remove(caminho_db)
        print("Banco antigo removido.")
    
    conn = sqlite3.connect(caminho_db)
    cursor = conn.cursor()
    
    print("\n--- Processando Tabelas Dimensão ---")
    
    for nome_tabela, info in TABELAS_DIMENSAO.items():
        arquivo_csv = os.path.join(CAMINHO_DADOS_BRUTOS, info['arquivo'])
        
        print(f"\n📁 {info['arquivo']}:")
        
        if os.path.exists(arquivo_csv):
            try:
                df = ler_csv_com_encoding_correto(arquivo_csv)
                
                # <<< CORREÇÃO CRÍTICA: Converter nomes das colunas para minúsculas >>>
                df.columns = [col.lower() for col in df.columns]
                print(f"   - Nomes de colunas convertidos para minúsculas: {', '.join(df.columns)}")

                df.to_sql(nome_tabela, conn, if_exists='replace', index=False)
                
                # Cria índice na chave primária (já em minúsculas)
                chave_primaria = info['chave_primaria']
                if chave_primaria in df.columns:
                    cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{nome_tabela}_{chave_primaria} ON {nome_tabela} ({chave_primaria})")
                
                print(f"   ✅ Tabela '{nome_tabela}' criada com {len(df)} registros.")
                
            except Exception as e:
                print(f"   ❌ Erro ao processar: {e}")
        else:
            print(f"   ⚠️  Arquivo não encontrado!")

    conn.commit()
    conn.close()
    
    end_time = time.time()
    print(f"\n⏱️  Tempo total: {end_time - start_time:.2f} segundos")
    print(f"💾 Banco de dimensões criado/atualizado em: {os.path.abspath(caminho_db)}")

if __name__ == "__main__":
    criar_banco_dimensoes()