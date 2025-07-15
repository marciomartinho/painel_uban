import sqlite3
import pandas as pd
import os
from tabulate import tabulate

# Configuração de caminhos
if os.path.basename(os.getcwd()) == 'scripts':
    BASE_DIR = os.path.dirname(os.getcwd())
else:
    BASE_DIR = os.getcwd()

CAMINHO_DB = os.path.join(BASE_DIR, 'dados', 'db', 'banco_saldo_receita.db')

def criar_visualizacao_pivot(conta_contabil='621200000', ano=None):
    """
    Cria uma visualização tipo pivot table (planilha Excel)
    com COUG nas linhas e INMES nas colunas
    
    Args:
        conta_contabil: Conta contábil para filtrar (padrão: 621200000)
        ano: Ano para filtrar (se None, pega o mais recente)
    """
    
    print("=" * 80)
    print(f"VISUALIZAÇÃO PIVOT - CONTA {conta_contabil}")
    print("=" * 80)
    
    # Conecta ao banco
    if not os.path.exists(CAMINHO_DB):
        print(f"❌ Banco de dados não encontrado: {CAMINHO_DB}")
        return
    
    conn = sqlite3.connect(CAMINHO_DB)
    
    try:
        # Se não especificou ano, pega o mais recente
        if ano is None:
            cursor = conn.cursor()
            cursor.execute("SELECT MAX(COEXERCICIO) FROM fato_saldos")
            ano = cursor.fetchone()[0]
        
        print(f"\n📅 Exercício: {ano}")
        print(f"📊 Conta Contábil: {conta_contabil}")
        
        # Query para buscar os dados
        query = """
        SELECT 
            COUG,
            INMES,
            SUM(saldo_contabil) as total_saldo
        FROM fato_saldos
        WHERE COCONTACONTABIL = ? AND COEXERCICIO = ?
        GROUP BY COUG, INMES
        ORDER BY COUG, INMES
        """
        
        # Carrega os dados
        df = pd.read_sql_query(query, conn, params=[conta_contabil, ano])
        
        if df.empty:
            print(f"\n⚠️  Nenhum dado encontrado para a conta {conta_contabil} no ano {ano}")
            return
        
        # Cria a pivot table
        pivot = df.pivot(index='COUG', columns='INMES', values='total_saldo')
        
        # Preenche valores nulos com 0
        pivot = pivot.fillna(0)
        
        # CALCULA O SALDO ACUMULADO - cada mês soma com os anteriores
        print("\n  - Calculando saldos acumulados...")
        for col in range(2, 13):  # Do mês 2 ao 12
            if col in pivot.columns and (col-1) in pivot.columns:
                pivot[col] = pivot[col] + pivot[col-1]
        
        # Adiciona total por linha (COUG)
        pivot['TOTAL'] = pivot.sum(axis=1)
        
        # Adiciona total por coluna (INMES)
        totais = pivot.sum()
        totais.name = 'TOTAL'
        pivot = pd.concat([pivot, totais.to_frame().T])
        
        # Formata os valores
        pivot_formatado = pivot.copy()
        for col in pivot_formatado.columns:
            pivot_formatado[col] = pivot_formatado[col].apply(lambda x: f"{x:,.2f}" if x != 0 else "-")
        
        # Exibe a tabela
        print("\n📊 TABELA PIVOT (COUG x INMES):")
        print("   Valores em R$")
        print()
        
        # Prepara os headers com nomes dos meses
        meses = {1: 'JAN', 2: 'FEV', 3: 'MAR', 4: 'ABR', 5: 'MAI', 6: 'JUN',
                 7: 'JUL', 8: 'AGO', 9: 'SET', 10: 'OUT', 11: 'NOV', 12: 'DEZ'}
        
        headers = ['COUG']
        for col in pivot_formatado.columns:
            if col == 'TOTAL':
                headers.append('TOTAL')
            else:
                headers.append(meses.get(col, str(col)))
        
        # Cria a tabela para exibição
        tabela_display = []
        for idx in pivot_formatado.index:
            linha = [str(idx)]
            for col in pivot_formatado.columns:
                linha.append(pivot_formatado.loc[idx, col])
            tabela_display.append(linha)
        
        # Exibe usando tabulate para melhor formatação
        print(tabulate(tabela_display, headers=headers, tablefmt='grid', stralign='right'))
        
        # Estatísticas adicionais
        print("\n📈 ESTATÍSTICAS:")
        
        # Top 5 COUGs com maior saldo total
        top_cougs = pivot.drop('TOTAL').sort_values('TOTAL', ascending=False).head(5)
        print("\n  Top 5 Unidades Gestoras (por saldo total):")
        for coug, total in zip(top_cougs.index, top_cougs['TOTAL']):
            print(f"    {coug}: R$ {total:,.2f}")
        
        # Mês com maior movimento
        totais_mes = pivot.loc['TOTAL'].drop('TOTAL')
        mes_max = totais_mes.idxmax()
        print(f"\n  Mês com maior saldo: {meses.get(mes_max, mes_max)} (R$ {totais_mes[mes_max]:,.2f})")
        
        # Total geral
        total_geral = pivot.loc['TOTAL', 'TOTAL']
        print(f"\n  💰 TOTAL GERAL: R$ {total_geral:,.2f}")
        
        # Salva em CSV opcional
        resposta = input("\n💾 Deseja salvar em CSV? (s/n): ")
        if resposta.lower() == 's':
            nome_arquivo = f"pivot_conta_{conta_contabil}_{ano}.csv"
            caminho_csv = os.path.join(BASE_DIR, 'dados', nome_arquivo)
            pivot.to_csv(caminho_csv, decimal=',', sep=';')
            print(f"✅ Arquivo salvo em: {caminho_csv}")
            
    except Exception as e:
        print(f"\n❌ Erro ao processar dados: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

def listar_contas_disponiveis():
    """Lista as contas contábeis disponíveis no banco"""
    
    if not os.path.exists(CAMINHO_DB):
        print(f"❌ Banco de dados não encontrado: {CAMINHO_DB}")
        return
    
    conn = sqlite3.connect(CAMINHO_DB)
    
    try:
        query = """
        SELECT DISTINCT 
            COCONTACONTABIL,
            COUNT(DISTINCT COUG) as qtd_ugs,
            SUM(saldo_contabil) as saldo_total
        FROM fato_saldos
        WHERE COEXERCICIO = (SELECT MAX(COEXERCICIO) FROM fato_saldos)
        GROUP BY COCONTACONTABIL
        ORDER BY ABS(saldo_total) DESC
        LIMIT 20
        """
        
        df = pd.read_sql_query(query, conn)
        
        print("\n📋 PRINCIPAIS CONTAS CONTÁBEIS (por saldo):")
        print(f"{'Conta':<15} | {'UGs':>5} | {'Saldo Total':>20}")
        print("-" * 45)
        
        for _, row in df.iterrows():
            print(f"{row['COCONTACONTABIL']:<15} | {row['qtd_ugs']:>5} | R$ {row['saldo_total']:>17,.2f}")
            
    finally:
        conn.close()

if __name__ == "__main__":
    print("VISUALIZADOR DE SALDOS - FORMATO PIVOT")
    print("=" * 40)
    
    # Lista contas disponíveis
    listar_contas_disponiveis()
    
    # Solicita entrada do usuário
    print("\n" + "=" * 40)
    conta = input("\nDigite a conta contábil (Enter para 621200000): ").strip()
    if not conta:
        conta = '621200000'
    
    ano = input("Digite o ano (Enter para o mais recente): ").strip()
    if ano:
        ano = int(ano)
    else:
        ano = None
    
    # Cria a visualização
    criar_visualizacao_pivot(conta, ano)
    