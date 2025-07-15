# verificar_bancos.py
import sqlite3
import os

bancos = {
    'saldos': 'dados/db/banco_saldo_receita.db',
    'lancamentos': 'dados/db/banco_lancamento_receita.db', 
    'dimensoes': 'dados/db/banco_dimensoes.db'
}

for nome, caminho in bancos.items():
    print(f"\n📊 BANCO: {nome.upper()}")
    print(f"📁 Arquivo: {caminho}")
    
    if os.path.exists(caminho):
        try:
            conn = sqlite3.connect(caminho)
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tabelas = cursor.fetchall()
            print(f"✅ Tabelas encontradas: {len(tabelas)}")
            for tabela in tabelas:
                print(f"   - {tabela[0]}")
            conn.close()
        except Exception as e:
            print(f"❌ Erro: {e}")
    else:
        print("❌ Arquivo não encontrado")

input("\nPressione Enter para fechar...")