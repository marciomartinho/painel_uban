import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import os

# Configuração de caminhos
if os.path.basename(os.getcwd()) == 'scripts':
    BASE_DIR = os.path.dirname(os.getcwd())
else:
    BASE_DIR = os.getcwd()

CAMINHO_DB = os.path.join(BASE_DIR, 'dados', 'db', 'banco_saldo_receita.db')

def criar_heatmap_saldos(conta_contabil='621200000', ano=None, top_n=20):
    """
    Cria um heatmap dos saldos por COUG x INMES
    
    Args:
        conta_contabil: Conta contábil para filtrar
        ano: Ano para filtrar (se None, pega o mais recente)
        top_n: Número de COUGs com maior movimento para exibir
    """
    
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
        
        # Query para buscar os dados
        query = """
        WITH coug_totais AS (
            SELECT 
                COUG,
                SUM(ABS(saldo_contabil)) as total_movimento
            FROM fato_saldos
            WHERE COCONTACONTABIL = ? AND COEXERCICIO = ?
            GROUP BY COUG
            ORDER BY total_movimento DESC
            LIMIT ?
        )
        SELECT 
            f.COUG,
            f.INMES,
            SUM(f.saldo_contabil) as total_saldo
        FROM fato_saldos f
        INNER JOIN coug_totais ct ON f.COUG = ct.COUG
        WHERE f.COCONTACONTABIL = ? AND f.COEXERCICIO = ?
        GROUP BY f.COUG, f.INMES
        ORDER BY f.COUG, f.INMES
        """
        
        # Carrega os dados
        df = pd.read_sql_query(query, conn, params=[conta_contabil, ano, top_n, conta_contabil, ano])
        
        if df.empty:
            print(f"\n⚠️  Nenhum dado encontrado para a conta {conta_contabil} no ano {ano}")
            return
        
        # Busca o nome da UG para melhor visualização
        query_nomes = """
        SELECT DISTINCT COUG, NOUG
        FROM fato_saldos
        WHERE COUG IN ({})
        """.format(','.join(['?'] * df['COUG'].nunique()))
        
        df_nomes = pd.read_sql_query(query_nomes, conn, params=df['COUG'].unique().tolist())
        
        # Cria a pivot table
        pivot = df.pivot(index='COUG', columns='INMES', values='total_saldo')
        pivot = pivot.fillna(0)
        
        # Adiciona os nomes das UGs
        pivot_com_nomes = pivot.copy()
        pivot_com_nomes.index = pivot_com_nomes.index.map(
            lambda x: f"{x} - {df_nomes[df_nomes['COUG'] == x]['NOUG'].iloc[0][:30]}" 
            if len(df_nomes[df_nomes['COUG'] == x]) > 0 else str(x)
        )
        
        # Configuração do plot
        plt.figure(figsize=(14, max(8, len(pivot) * 0.4)))
        
        # Cria o heatmap
        # Define limites para a escala de cores
        vmax = pivot.values.max()
        vmin = pivot.values.min()
        
        # Se houver valores negativos e positivos, centraliza no zero
        if vmin < 0 and vmax > 0:
            limite = max(abs(vmin), abs(vmax))
            vmin, vmax = -limite, limite
            cmap = 'RdBu_r'  # Vermelho para negativo, azul para positivo
        else:
            cmap = 'Blues' if vmax >= 0 else 'Reds_r'
        
        # Cria o heatmap
        ax = sns.heatmap(
            pivot_com_nomes,
            cmap=cmap,
            center=0 if (vmin < 0 and vmax > 0) else None,
            vmin=vmin,
            vmax=vmax,
            fmt='.0f',
            linewidths=0.5,
            cbar_kws={'label': 'Saldo Contábil (R$)'},
            annot=True,
            annot_kws={'size': 8}
        )
        
        # Configurações do gráfico
        plt.title(f'Saldo Contábil por Unidade Gestora e Mês\nConta: {conta_contabil} - Exercício: {ano}', 
                 fontsize=14, pad=20)
        
        # Ajusta labels do eixo X (meses)
        meses = ['JAN', 'FEV', 'MAR', 'ABR', 'MAI', 'JUN', 
                'JUL', 'AGO', 'SET', 'OUT', 'NOV', 'DEZ']
        
        x_labels = []
        for col in pivot.columns:
            if 1 <= col <= 12:
                x_labels.append(meses[col-1])
            else:
                x_labels.append(str(col))
        
        ax.set_xticklabels(x_labels, rotation=0)
        ax.set_xlabel('Mês', fontsize=12)
        ax.set_ylabel('Unidade Gestora (COUG)', fontsize=12)
        
        # Ajusta o layout
        plt.tight_layout()
        
        # Salva o gráfico
        nome_arquivo = f"heatmap_conta_{conta_contabil}_{ano}.png"
        caminho_img = os.path.join(BASE_DIR, 'dados', nome_arquivo)
        plt.savefig(caminho_img, dpi=300, bbox_inches='tight')
        print(f"\n✅ Gráfico salvo em: {caminho_img}")
        
        # Exibe o gráfico
        plt.show()
        
        # Estatísticas
        print(f"\n📊 ESTATÍSTICAS - Conta {conta_contabil} - Ano {ano}")
        print("=" * 50)
        
        total_geral = pivot.sum().sum()
        print(f"💰 Total Geral: R$ {total_geral:,.2f}")
        
        # Mês com maior saldo
        totais_mes = pivot.sum()
        mes_max = totais_mes.idxmax()
        print(f"📅 Mês com maior saldo: {meses[mes_max-1] if 1 <= mes_max <= 12 else mes_max} (R$ {totais_mes[mes_max]:,.2f})")
        
        # UG com maior saldo total
        totais_ug = pivot.sum(axis=1)
        ug_max = totais_ug.idxmax()
        nome_ug_max = pivot_com_nomes.index[pivot.index.tolist().index(ug_max)]
        print(f"🏢 UG com maior saldo total: {nome_ug_max} (R$ {totais_ug[ug_max]:,.2f})")
        
    except Exception as e:
        print(f"\n❌ Erro ao processar dados: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

def criar_grafico_evolucao(conta_contabil='621200000', ano=None):
    """
    Cria gráfico de linha mostrando evolução mensal do saldo
    """
    
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
        
        # Query para evolução mensal
        query = """
        SELECT 
            INMES,
            SUM(saldo_contabil) as saldo_total,
            COUNT(DISTINCT COUG) as qtd_ugs
        FROM fato_saldos
        WHERE COCONTACONTABIL = ? AND COEXERCICIO = ?
        GROUP BY INMES
        ORDER BY INMES
        """
        
        df = pd.read_sql_query(query, conn, params=[conta_contabil, ano])
        
        if df.empty:
            print(f"\n⚠️  Nenhum dado encontrado")
            return
        
        # Cria o gráfico
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), height_ratios=[2, 1])
        
        # Gráfico principal - Evolução do saldo
        meses = ['JAN', 'FEV', 'MAR', 'ABR', 'MAI', 'JUN', 
                'JUL', 'AGO', 'SET', 'OUT', 'NOV', 'DEZ']
        
        x_labels = [meses[m-1] if 1 <= m <= 12 else str(m) for m in df['INMES']]
        x_pos = range(len(df))
        
        # Barras com cores diferentes para positivo/negativo
        colors = ['green' if x > 0 else 'red' for x in df['saldo_total']]
        bars = ax1.bar(x_pos, df['saldo_total'], color=colors, alpha=0.7, edgecolor='black')
        
        # Linha de tendência
        ax1.plot(x_pos, df['saldo_total'], 'b-', linewidth=2, marker='o', markersize=8)
        
        # Adiciona valores nas barras
        for i, (bar, valor) in enumerate(zip(bars, df['saldo_total'])):
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width()/2., height,
                    f'R$ {valor/1e6:.1f}M' if abs(valor) > 1e6 else f'R$ {valor/1e3:.0f}K',
                    ha='center', va='bottom' if valor > 0 else 'top', fontsize=8)
        
        ax1.set_title(f'Evolução Mensal do Saldo - Conta {conta_contabil}\nExercício {ano}', fontsize=14)
        ax1.set_xlabel('Mês', fontsize=12)
        ax1.set_ylabel('Saldo Contábil (R$)', fontsize=12)
        ax1.set_xticks(x_pos)
        ax1.set_xticklabels(x_labels)
        ax1.grid(True, alpha=0.3, axis='y')
        ax1.axhline(y=0, color='black', linestyle='-', linewidth=0.5)
        
        # Formata eixo Y
        ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'R$ {x/1e6:.1f}M' if abs(x) > 1e6 else f'R$ {x/1e3:.0f}K'))
        
        # Gráfico secundário - Quantidade de UGs
        ax2.bar(x_pos, df['qtd_ugs'], color='skyblue', alpha=0.7, edgecolor='black')
        ax2.set_xlabel('Mês', fontsize=12)
        ax2.set_ylabel('Qtd UGs', fontsize=12)
        ax2.set_xticks(x_pos)
        ax2.set_xticklabels(x_labels)
        ax2.grid(True, alpha=0.3, axis='y')
        
        # Adiciona valores
        for i, (x, y) in enumerate(zip(x_pos, df['qtd_ugs'])):
            ax2.text(x, y, str(y), ha='center', va='bottom', fontsize=8)
        
        plt.tight_layout()
        
        # Salva o gráfico
        nome_arquivo = f"evolucao_conta_{conta_contabil}_{ano}.png"
        caminho_img = os.path.join(BASE_DIR, 'dados', nome_arquivo)
        plt.savefig(caminho_img, dpi=300, bbox_inches='tight')
        print(f"\n✅ Gráfico salvo em: {caminho_img}")
        
        plt.show()
        
        # Estatísticas
        print(f"\n📊 RESUMO DA EVOLUÇÃO")
        print("=" * 40)
        print(f"Saldo acumulado: R$ {df['saldo_total'].sum():,.2f}")
        print(f"Média mensal: R$ {df['saldo_total'].mean():,.2f}")
        print(f"Maior saldo: R$ {df['saldo_total'].max():,.2f} ({x_labels[df['saldo_total'].idxmax()]})")
        print(f"Menor saldo: R$ {df['saldo_total'].min():,.2f} ({x_labels[df['saldo_total'].idxmin()]})")
        
    except Exception as e:
        print(f"\n❌ Erro: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    print("VISUALIZADOR DE SALDOS - HEATMAP E GRÁFICOS")
    print("=" * 45)
    
    # Menu de opções
    print("\nOpções de visualização:")
    print("1. Heatmap (COUG x INMES)")
    print("2. Gráfico de Evolução Mensal")
    print("3. Ambos")
    
    opcao = input("\nEscolha uma opção (1-3): ").strip()
    
    # Solicita parâmetros
    conta = input("\nDigite a conta contábil (Enter para 621200000): ").strip()
    if not conta:
        conta = '621200000'
    
    ano = input("Digite o ano (Enter para o mais recente): ").strip()
    if ano:
        ano = int(ano)
    else:
        ano = None
    
    # Executa conforme opção
    if opcao == '1':
        top_n = input("Quantas UGs exibir? (Enter para 20): ").strip()
        top_n = int(top_n) if top_n else 20
        criar_heatmap_saldos(conta, ano, top_n)
    elif opcao == '2':
        criar_grafico_evolucao(conta, ano)
    elif opcao == '3':
        top_n = input("Quantas UGs exibir no heatmap? (Enter para 20): ").strip()
        top_n = int(top_n) if top_n else 20
        criar_heatmap_saldos(conta, ano, top_n)
        criar_grafico_evolucao(conta, ano)
    else:
        print("Opção inválida!")