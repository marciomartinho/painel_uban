# teste_conexao_simples.py
import psycopg2

urls_para_testar = [
    "postgresql://postgres:uzVUcFKomVccdwGGtwrGeyOHWcrjxiIu@ballast.proxy.rlwy.net:11664/railway",
    # Vamos testar variações se a primeira não funcionar
]

for i, url in enumerate(urls_para_testar, 1):
    print(f"\n🧪 Teste {i}: {url}")
    try:
        conn = psycopg2.connect(url, connect_timeout=10)
        print("✅ SUCESSO!")
        conn.close()
        break
    except Exception as e:
        print(f"❌ Erro: {e}")

input("Pressione Enter...")