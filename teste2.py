# teste2.py
print("🔍 Testando se a aplicação carrega...")

try:
    from main import app
    print("✅ Sucesso! A aplicação Flask foi carregada!")
    print(f"Nome da aplicação: {app.name}")
except Exception as e:
    print(f"❌ Erro: {e}")

print("\n✅ Teste concluído!")
input("Pressione Enter para fechar...")