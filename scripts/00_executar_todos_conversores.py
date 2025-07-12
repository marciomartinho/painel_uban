import os
import subprocess
import sys
import time

# --- CONFIGURAÇÃO ---
if os.path.basename(os.getcwd()) == 'scripts':
    SCRIPTS_DIR = os.getcwd()
    BASE_DIR = os.path.dirname(os.getcwd())
else:
    SCRIPTS_DIR = os.path.join(os.getcwd(), 'scripts')
    BASE_DIR = os.getcwd()

def executar_script(nome_script):
    """Executa um script Python e retorna o status"""
    print(f"\n{'='*60}")
    print(f"Executando: {nome_script}")
    print(f"{'='*60}")
    
    caminho_script = os.path.join(SCRIPTS_DIR, nome_script)
    
    if not os.path.exists(caminho_script):
        print(f"❌ Script não encontrado: {caminho_script}")
        return False
    
    try:
        # Executa o script
        resultado = subprocess.run(
            [sys.executable, caminho_script],
            capture_output=True,
            text=True,
            cwd=BASE_DIR  # Executa da raiz do projeto
        )
        
        # Mostra a saída
        if resultado.stdout:
            print(resultado.stdout)
        
        if resultado.stderr:
            print("ERROS:")
            print(resultado.stderr)
        
        if resultado.returncode == 0:
            print(f"✅ {nome_script} executado com sucesso!")
            return True
        else:
            print(f"❌ {nome_script} falhou com código: {resultado.returncode}")
            return False
            
    except Exception as e:
        print(f"❌ Erro ao executar {nome_script}: {e}")
        return False

def main():
    """Executa todos os conversores em ordem"""
    
    print("🚀 EXECUTANDO TODOS OS CONVERSORES")
    print("="*60)
    
    start_time = time.time()
    
    # Lista de scripts para executar em ordem
    scripts = [
        "01_conversor_dimensoes.py",
        "02_conversor_lancamentos.py",
        "03_conversor_saldos.py"
    ]
    
    # Cria estrutura de pastas
    print("📁 Criando estrutura de pastas...")
    os.makedirs(os.path.join(BASE_DIR, 'dados', 'db'), exist_ok=True)
    print("✅ Estrutura criada!")
    
    # Executa cada script
    sucessos = 0
    falhas = 0
    
    for script in scripts:
        if executar_script(script):
            sucessos += 1
        else:
            falhas += 1
            resposta = input(f"\n⚠️  {script} falhou. Continuar? (s/n): ")
            if resposta.lower() != 's':
                print("❌ Execução cancelada!")
                break
    
    # Resumo final
    end_time = time.time()
    tempo_total = end_time - start_time
    
    print("\n" + "="*60)
    print("📊 RESUMO DA EXECUÇÃO")
    print("="*60)
    print(f"✅ Scripts executados com sucesso: {sucessos}")
    print(f"❌ Scripts com falha: {falhas}")
    print(f"⏱️  Tempo total: {tempo_total:.2f} segundos")
    
    # Lista os arquivos criados
    print("\n💾 Arquivos de banco de dados criados:")
    db_path = os.path.join(BASE_DIR, 'dados', 'db')
    if os.path.exists(db_path):
        for arquivo in os.listdir(db_path):
            if arquivo.endswith('.db'):
                caminho_completo = os.path.join(db_path, arquivo)
                tamanho = os.path.getsize(caminho_completo) / 1024 / 1024  # MB
                print(f"   - {arquivo} ({tamanho:.2f} MB)")
    
    print("\n✨ Processo concluído!")

if __name__ == "__main__":
    main()