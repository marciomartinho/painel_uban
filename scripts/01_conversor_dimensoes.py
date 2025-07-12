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
CAMINHO_DADOS_BRUTOS = os.path.join(BASE_DIR, 'dados', 'dados_brutos', 'dimensao')
CAMINHO_DB = os.path.join(BASE_DIR, 'dados', 'db')

# Criar pasta db se não existir
os.makedirs(CAMINHO_DB, exist_ok=True)

# Tabelas Dimensão
TABELAS_DIMENSAO = {
    'categorias': {
        'arquivo': 'receita_categoria.csv',
        'chave_primaria': 'COCATEGORIARECEITA',
        'descricao': 'Categorias de Receita (1º dígito do COCONTACORRENTE)'
    },
    'origens': {
        'arquivo': 'receita_origem.csv',
        'chave_primaria': 'COFONTERECEITA',
        'descricao': 'Origens de Receita (2 primeiros dígitos do COCONTACORRENTE)'
    },
    'especies': {
        'arquivo': 'receita_especie.csv',
        'chave_primaria': 'COSUBFONTERECEITA',
        'descricao': 'Espécies de Receita (3 primeiros dígitos do COCONTACORRENTE)'
    },
    'especificacoes': {
        'arquivo': 'receita_especificacao.csv',
        'chave_primaria': 'CORUBRICA',
        'descricao': 'Especificações/Rubricas (4 primeiros dígitos do COCONTACORRENTE)'
    },
    'alineas': {
        'arquivo': 'receita_alinea.csv',
        'chave_primaria': 'COALINEA',
        'descricao': 'Alíneas (6 primeiros dígitos do COCONTACORRENTE)'
    },
    'fontes': {
        'arquivo': 'fonte.csv',
        'chave_primaria': 'COFONTE',
        'descricao': 'Fontes de Recursos (9º ao 17º dígito do COCONTACORRENTE)'
    },
    'contas': {
        'arquivo': 'contacontabil.csv',
        'chave_primaria': 'COCONTACONTABIL',
        'descricao': 'Contas Contábeis'
    },
    'unidades_gestoras': {
        'arquivo': 'unidadegestora.csv',
        'chave_primaria': 'COUG',
        'descricao': 'Unidades Gestoras'
    }
}

def criar_banco_dimensoes():
    """Cria o banco de dados com todas as tabelas de dimensão"""
    
    print("=" * 60)
    print("CONVERSOR DE TABELAS DIMENSÃO")
    print("=" * 60)
    
    start_time = time.time()
    
    # Caminho do banco
    caminho_db = os.path.join(CAMINHO_DB, 'banco_dimensoes.db')
    
    print(f"\nCriando banco de dimensões em: {caminho_db}")
    
    # Remove banco antigo se existir
    if os.path.exists(caminho_db):
        resposta = input("\nBanco de dimensões já existe. Deseja substituí-lo? (s/n): ")
        if resposta.lower() != 's':
            print("Operação cancelada.")
            return
        os.remove(caminho_db)
        print("Banco antigo removido.")
    
    # Conecta ao banco
    conn = sqlite3.connect(caminho_db)
    cursor = conn.cursor()
    
    print("\n--- Processando Tabelas Dimensão ---")
    
    total_registros = 0
    tabelas_criadas = 0
    
    for nome_tabela, info in TABELAS_DIMENSAO.items():
        arquivo_csv = os.path.join(CAMINHO_DADOS_BRUTOS, info['arquivo'])
        
        print(f"\n📁 {info['arquivo']}:")
        print(f"   Descrição: {info['descricao']}")
        
        if os.path.exists(arquivo_csv):
            try:
                # Lê o CSV
                df = pd.read_csv(arquivo_csv, encoding='latin1', on_bad_lines='skip')
                
                # Informações sobre a tabela
                print(f"   Registros: {len(df)}")
                print(f"   Colunas: {', '.join(df.columns.tolist())}")
                
                # Salva no banco
                df.to_sql(nome_tabela, conn, if_exists='replace', index=False)
                
                # Cria índice na chave primária
                try:
                    cursor.execute(f"CREATE INDEX idx_{nome_tabela}_{info['chave_primaria'].lower()} ON {nome_tabela} ({info['chave_primaria']})")
                except:
                    pass  # Índice pode já existir
                
                print(f"   ✅ Tabela '{nome_tabela}' criada com sucesso!")
                
                total_registros += len(df)
                tabelas_criadas += 1
                
            except Exception as e:
                print(f"   ❌ Erro ao processar: {e}")
        else:
            print(f"   ⚠️  Arquivo não encontrado!")
    
    # Cria views úteis para facilitar consultas
    print("\n--- Criando Views Auxiliares ---")
    
    # View para hierarquia completa de receitas
    try:
        cursor.execute("""
        CREATE VIEW v_hierarquia_receita AS
        SELECT 
            cat.COCATEGORIARECEITA,
            cat.NOCATEGORIARECEITA,
            ori.COFONTERECEITA,
            ori.NOFONTERECEITA,
            esp.COSUBFONTERECEITA,
            esp.NOSUBFONTERECEITA,
            rub.CORUBRICA,
            rub.NORUBRICA,
            ali.COALINEA,
            ali.NOALINEA
        FROM categorias cat
        LEFT JOIN origens ori ON substr(ori.COFONTERECEITA, 1, 1) = cat.COCATEGORIARECEITA
        LEFT JOIN especies esp ON substr(esp.COSUBFONTERECEITA, 1, 2) = ori.COFONTERECEITA
        LEFT JOIN especificacoes rub ON substr(rub.CORUBRICA, 1, 3) = esp.COSUBFONTERECEITA
        LEFT JOIN alineas ali ON substr(ali.COALINEA, 1, 4) = rub.CORUBRICA
        """)
        print("   ✅ View 'v_hierarquia_receita' criada")
    except Exception as e:
        print(f"   ⚠️  Erro ao criar view: {e}")
    
    conn.commit()
    
    # Estatísticas finais
    print("\n--- Resumo ---")
    print(f"✅ Tabelas criadas: {tabelas_criadas}")
    print(f"📊 Total de registros: {total_registros:,}")
    
    # Lista todas as tabelas criadas
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tabelas = cursor.fetchall()
    print(f"\n📋 Tabelas no banco:")
    for tabela in tabelas:
        cursor.execute(f"SELECT COUNT(*) FROM {tabela[0]}")
        count = cursor.fetchone()[0]
        print(f"   - {tabela[0]}: {count:,} registros")
    
    conn.close()
    
    end_time = time.time()
    print(f"\n⏱️  Tempo total: {end_time - start_time:.2f} segundos")
    print(f"💾 Banco criado em: {os.path.abspath(caminho_db)}")

if __name__ == "__main__":
    criar_banco_dimensoes()