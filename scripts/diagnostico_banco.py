import sqlite3
import pandas as pd
import os

# Caminho do banco
if os.path.basename(os.getcwd()) == 'scripts':
    BASE_DIR = os.path.dirname(os.getcwd())
else:
    BASE_DIR = os.getcwd()

caminho_db = os.path.join(BASE_DIR, 'banco_lancamento_receita.db')

print("=" * 60)
print("DIAGNÓSTICO DO BANCO DE DADOS")
print("=" * 60)

conn = sqlite3.connect(caminho_db)

# 1. Verificar tabelas existentes
print("\n1. TABELAS EXISTENTES:")
cursor = conn.cursor()
cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
tabelas = cursor.fetchall()
for tabela in tabelas:
    print(f"   - {tabela[0]}")

# 2. Verificar estrutura da tabela fato_saldos
print("\n2. ESTRUTURA DA TABELA fato_saldos:")
try:
    cursor.execute("PRAGMA table_info(fato_saldos)")
    colunas = cursor.fetchall()
    for col in colunas:
        print(f"   - {col[1]} ({col[2]})")
except Exception as e:
    print(f"   ERRO: {e}")

# 3. Verificar se há dados e mostrar amostra
print("\n3. DADOS NA TABELA fato_saldos:")
try:
    # Total de registros
    cursor.execute("SELECT COUNT(*) FROM fato_saldos")
    total = cursor.fetchone()[0]
    print(f"   Total de registros: {total}")
    
    # Amostra dos dados
    print("\n   Amostra (3 primeiros registros):")
    df = pd.read_sql_query("SELECT * FROM fato_saldos LIMIT 3", conn)
    print(df.to_string())
    
    # Verificar colunas específicas
    print("\n4. VERIFICANDO COLUNAS ESPERADAS:")
    colunas_esperadas = [
        'PREVISAO INICIAL',
        'PREVISAO ATUALIZADA LIQUIDA', 
        'RECEITA LIQUIDA',
        'CATEGORIARECEITA',
        'ORIGEM',
        'ESPECIE',
        'INMES',
        'COEXERCICIO'
    ]
    
    for col in colunas_esperadas:
        try:
            cursor.execute(f'SELECT "{col}" FROM fato_saldos LIMIT 1')
            print(f"   ✓ Coluna '{col}' existe")
        except:
            print(f"   ✗ Coluna '{col}' NÃO EXISTE")
    
    # Listar todas as colunas que contêm "PREVISAO" ou "RECEITA"
    print("\n5. COLUNAS RELACIONADAS A VALORES:")
    cursor.execute("PRAGMA table_info(fato_saldos)")
    colunas = cursor.fetchall()
    for col in colunas:
        nome_col = col[1]
        if 'PREVISAO' in nome_col.upper() or 'RECEITA' in nome_col.upper() or 'SALDO' in nome_col.upper():
            # Pega um valor de exemplo
            cursor.execute(f'SELECT "{nome_col}" FROM fato_saldos WHERE "{nome_col}" IS NOT NULL AND "{nome_col}" != 0 LIMIT 1')
            exemplo = cursor.fetchone()
            valor_exemplo = exemplo[0] if exemplo else "NULL"
            print(f"   - {nome_col}: {valor_exemplo}")
    
    # Verificar períodos disponíveis
    print("\n6. PERÍODOS DISPONÍVEIS:")
    cursor.execute("SELECT DISTINCT COEXERCICIO, INMES FROM fato_saldos ORDER BY COEXERCICIO, INMES")
    periodos = cursor.fetchall()
    for ano, mes in periodos:
        print(f"   - {mes}/{ano}")
        
except Exception as e:
    print(f"   ERRO: {e}")
    import traceback
    traceback.print_exc()

# 7. Verificar tabelas de dimensão
print("\n7. VERIFICANDO TABELAS DE DIMENSÃO:")
for tabela in ['categorias', 'origens', 'especies']:
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {tabela}")
        total = cursor.fetchone()[0]
        print(f"   - {tabela}: {total} registros")
    except:
        print(f"   - {tabela}: NÃO EXISTE")

conn.close()

print("\n" + "=" * 60)