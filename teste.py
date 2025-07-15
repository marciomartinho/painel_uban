# teste.py
print("🔍 Testando se a aplicação carrega...")

try:
    from app import app
    print("✅ Sucesso! A aplicação Flask foi carregada!")
    print(f"Nome da aplicação: {app.name}")
except Exception as e:
    print(f"❌ Erro: {e}")
    print("Verifique se o arquivo app.py existe na mesma pasta")

print("\n✅ Teste concluído!")
input("Pressione Enter para fechar...")