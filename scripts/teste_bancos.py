import sqlite3
import os
import pandas as pd

# Configuração
if os.path.basename(os.getcwd()) == 'scripts':
    BASE_DIR = os.path.dirname(os.getcwd())
else:
    BASE_DIR = os.getcwd()

DB_PATH = os.path.join(BASE_DIR, 'dados', 'db')

def testar_banco(nome_banco, queries_teste):
    """Testa um banco de dados específico"""
    caminho = os.path.join(DB_PATH, nome_banco)
    
    print(f"\n{'='*60}")
    print(f"Testando: {nome_banco}")
    print(f"{'='*60}")
    
    if not os.path.exists(caminho):
        print(f"❌ Banco não encontrado em: {caminho}")
        return False
    
    print(f"✅ Banco encontrado!")
    print(f"📁 Tamanho: {os.path.getsize(caminho) / 1024 / 1024:.2f} MB")
    
    try:
        conn = sqlite3.connect(caminho)
        cursor = conn.cursor()
        
        # Lista tabelas
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tabelas = cursor.fetchall()
        print(f"\n📋 Tabelas encontradas:")
        for tabela in tabelas:
            cursor.execute(f"SELECT COUNT(*) FROM {tabela[0]}")
            count = cursor.fetchone()[0]
            print(f"   - {tabela[0]}: {count:,} registros")
        
        # Executa queries de teste
        print(f"\n🔍 Executando queries de teste:")
        for descricao, query in queries_teste.items():
            print(f"\n{descricao}:")
            try:
                df = pd.read_sql_query(query, conn)
                print(df.to_string())
            except Exception as e:
                print(f"❌ Erro: {e}")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Erro ao conectar: {e}")
        return False

def main():
    print("🧪 TESTE DOS BANCOS DE DADOS")
    print("="*60)
    
    # Testa banco de dimensões
    testar_banco('banco_dimensoes.db', {
        "Amostra de categorias": "SELECT * FROM categorias LIMIT 5",
        "Total de origens": "SELECT COUNT(*) as total FROM origens",
        "Hierarquia exemplo": """
            SELECT 
                c.COCATEGORIARECEITA,
                c.NOCATEGORIARECEITA,
                COUNT(DISTINCT o.COFONTERECEITA) as total_origens
            FROM categorias c
            LEFT JOIN origens o ON o.COFONTERECEITA LIKE c.COCATEGORIARECEITA || '%'
            GROUP BY c.COCATEGORIARECEITA
        """
    })
    
    # Testa banco de lançamentos
    testar_banco('banco_lancamento_receita.db', {
        "Períodos disponíveis": "SELECT * FROM dim_tempo ORDER BY COEXERCICIO DESC, INMES DESC LIMIT 10",
        "Campos extraídos (amostra)": """
            SELECT 
                COCONTACORRENTE,
                CATEGORIARECEITA,
                COFONTERECEITA,
                COSUBFONTERECEITA,
                CORUBRICA,
                COALINEA,
                COFONTE
            FROM lancamentos 
            LIMIT 3
        """,
        "Total por exercício": """
            SELECT 
                COEXERCICIO,
                COUNT(*) as total_lancamentos,
                SUM(VALANCAMENTO) as valor_total
            FROM lancamentos
            GROUP BY COEXERCICIO
        """
    })
    
    # Testa banco de saldos
    testar_banco('banco_saldo_receita.db', {
        "Estrutura (5 primeiras colunas)": "PRAGMA table_info(fato_saldos)",
        "Períodos disponíveis": "SELECT * FROM dim_tempo ORDER BY COEXERCICIO DESC, INMES DESC LIMIT 10",
        "Resumo por categoria (último período)": """
            SELECT 
                CATEGORIARECEITA,
                MAX(NOCATEGORIARECEITA) as CATEGORIA,
                SUM("PREVISAO INICIAL") as PREVISAO,
                SUM("RECEITA LIQUIDA") as REALIZADO
            FROM fato_saldos
            WHERE COEXERCICIO = 2025 AND INMES = 6
            GROUP BY CATEGORIARECEITA
        """
    })
    
    # Teste de JOIN entre bancos
    print(f"\n{'='*60}")
    print("Testando JOIN entre bancos")
    print(f"{'='*60}")
    
    try:
        conn = sqlite3.connect(os.path.join(DB_PATH, 'banco_saldo_receita.db'))
        conn.execute(f"ATTACH DATABASE '{os.path.join(DB_PATH, 'banco_dimensoes.db')}' AS dim")
        
        query = """
        SELECT 
            s.CATEGORIARECEITA,
            s.NOCATEGORIARECEITA as nome_saldo,
            d.NOCATEGORIARECEITA as nome_dimensao,
            COUNT(*) as registros
        FROM fato_saldos s
        LEFT JOIN dim.categorias d ON s.CATEGORIARECEITA = d.COCATEGORIARECEITA
        GROUP BY s.CATEGORIARECEITA
        LIMIT 5
        """
        
        df = pd.read_sql_query(query, conn)
        print("✅ JOIN entre bancos funcionando!")
        print(df.to_string())
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Erro no JOIN: {e}")
    
    print("\n✨ Teste concluído!")

if __name__ == "__main__":
    main()