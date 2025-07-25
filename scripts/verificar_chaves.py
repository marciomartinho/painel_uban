# scripts/verificar_chaves.py
import sqlite3
import os
import json

# --- CONFIGURAÇÃO ---
if os.path.basename(os.getcwd()) == 'scripts':
    BASE_DIR = os.path.dirname(os.getcwd())
else:
    BASE_DIR = os.getcwd()

CAMINHO_DB = os.path.join(BASE_DIR, 'dados', 'db')
CAMINHO_BANCO = os.path.join(CAMINHO_DB, 'banco_dimensoes.db')
ARQUIVO_CHAVES = os.path.join(CAMINHO_DB, 'chaves_primarias.json')

print("=" * 60)
print("SCRIPT DE VERIFICAÇÃO DE CHAVES PRIMÁRIAS")
print("=" * 60)

# 1. Verifica as tabelas no banco de dados
print("\n--- 1. Analisando o banco de dados ---")
if not os.path.exists(CAMINHO_BANCO):
    print(f"❌ ERRO: O arquivo do banco de dados não foi encontrado em '{CAMINHO_BANCO}'")
else:
    try:
        conn = sqlite3.connect(CAMINHO_BANCO)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
        tabelas = [row[0] for row in cursor.fetchall()]
        conn.close()
        
        print(f"✅ Banco de dados encontrado com {len(tabelas)} tabelas:")
        for tabela in tabelas:
            print(f"  - {tabela}")
    except Exception as e:
        print(f"❌ ERRO ao ler o banco de dados: {e}")

# 2. Verifica o arquivo de chaves primárias
print("\n--- 2. Analisando o arquivo de configuração de chaves ---")
if not os.path.exists(ARQUIVO_CHAVES):
    print(f"❌ ERRO: O arquivo 'chaves_primarias.json' não foi encontrado em '{CAMINHO_DB}'")
    print("\n   Isso é provavelmente a causa do problema. O conversor precisa ser executado para criar este arquivo.")
else:
    try:
        with open(ARQUIVO_CHAVES, 'r', encoding='utf-8') as f:
            chaves = json.load(f)
        
        print(f"✅ Arquivo 'chaves_primarias.json' encontrado com {len(chaves)} chaves salvas:")
        for tabela, chave in chaves.items():
            print(f"  - Tabela: '{tabela}' -> Chave: '{chave}'")
    except Exception as e:
        print(f"❌ ERRO ao ler o arquivo JSON: {e}")

print("\n" + "=" * 60)
print("Compare as duas listas acima. Os nomes das tabelas devem corresponder exatamente.")