# app/relatorios/balanco_orcamentario_receita.py
"""
Relatório de Balanço Orçamentário da Receita - Versão Simplificada
Demonstrativo consolidado comparativo entre previsão e realização das receitas
"""

import sqlite3
import os
from datetime import datetime
from typing import Dict, List

# Importa os módulos necessários
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from modulos.regras_contabeis_receita import get_filtro_conta
from modulos.periodo import obter_periodo_referencia
from modulos.formatacao import formatar_moeda, formatar_percentual


class BalancoOrcamentarioReceita:
    """Classe para gerar o Balanço Orçamentário da Receita Consolidado"""
    
    def __init__(self, caminho_db: str):
        self.caminho_db = caminho_db
        self.periodo = obter_periodo_referencia()
        
    def conectar_db(self):
        """Estabelece conexão com o banco de dados e anexa dimensões"""
        conn = sqlite3.connect(self.caminho_db)
        
        # Tenta anexar o banco de dimensões
        caminho_dimensoes = os.path.join(os.path.dirname(self.caminho_db), 'banco_dimensoes.db')
        if os.path.exists(caminho_dimensoes):
            try:
                conn.execute(f"ATTACH DATABASE '{caminho_dimensoes}' AS dimensoes")
                print(f"Banco de dimensões anexado com sucesso")
            except Exception as e:
                print(f"Aviso: Não foi possível anexar banco de dimensões: {e}")
        else:
            print(f"Aviso: Banco de dimensões não encontrado em {caminho_dimensoes}")
        
        return conn
    
    def verificar_estrutura_banco(self, conn):
        """Verifica a estrutura do banco de dados"""
        cursor = conn.cursor()
        
        # Verifica tabelas existentes no banco principal
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tabelas_principais = [row[0] for row in cursor.fetchall()]
        
        # Verifica tabelas no banco de dimensões (se anexado)
        tabelas_dimensoes = []
        tem_dimensoes_anexadas = False
        try:
            cursor.execute("SELECT name FROM dimensoes.sqlite_master WHERE type='table'")
            tabelas_dimensoes = [row[0] for row in cursor.fetchall()]
            tem_dimensoes_anexadas = len(tabelas_dimensoes) > 0
        except:
            pass
        
        # Verifica colunas da fato_saldos
        colunas_fato = []
        if 'fato_saldos' in tabelas_principais:
            cursor.execute("PRAGMA table_info(fato_saldos)")
            colunas_fato = [row[1] for row in cursor.fetchall()]
        
        return {
            'tabelas': tabelas_principais,
            'tabelas_dimensoes': tabelas_dimensoes,
            'colunas_fato': colunas_fato,
            'tem_dimensoes': tem_dimensoes_anexadas,
            'tem_colunas_calculadas': any(col in colunas_fato for col in ['PREVISAO INICIAL', 'RECEITA LIQUIDA'])
        }
    
    def executar_relatorio(self) -> Dict:
        """
        Executa o relatório e retorna os dados formatados
        
        Returns:
            Dict com estrutura do relatório consolidado
        """
        with self.conectar_db() as conn:
            estrutura = self.verificar_estrutura_banco(conn)
            
            resultado = {
                'periodo': self.periodo,
                'dados': self._buscar_dados_consolidados(conn, estrutura),
                'totais': {},
                'data_geracao': datetime.now().strftime('%d/%m/%Y %H:%M')
            }
            
            # Calcula totais
            resultado['totais'] = self._calcular_totais(resultado['dados'])
            
            return resultado
    
    def _buscar_dados_consolidados(self, conn: sqlite3.Connection, estrutura: Dict) -> List[Dict]:
        """
        Busca os dados consolidados do demonstrativo
        
        Returns:
            Lista com dados hierárquicos (categoria -> fonte)
        """
        if estrutura['tem_colunas_calculadas']:
            query = self._query_colunas_calculadas(estrutura)
        else:
            query = self._query_contas_contabeis(estrutura)
        
        cursor = conn.execute(query)
        
        # Organiza dados em estrutura hierárquica
        dados_hierarquicos = {}
        
        for row in cursor:
            cat_codigo = row[0]
            cat_nome = row[1] or f"Categoria {cat_codigo}"
            fonte_codigo = row[2]
            fonte_nome = row[3] or f"Fonte {fonte_codigo}"
            
            # Cria entrada para categoria se não existir
            if cat_codigo not in dados_hierarquicos:
                dados_hierarquicos[cat_codigo] = {
                    'codigo': cat_codigo,
                    'descricao': cat_nome,
                    'nivel': 1,
                    'previsao_inicial': 0,
                    'previsao_atualizada': 0,
                    'receita_atual': 0,
                    'receita_anterior': 0,
                    'subcategorias': []
                }
            
            # Adiciona valores à categoria
            dados_hierarquicos[cat_codigo]['previsao_inicial'] += row[4] or 0
            dados_hierarquicos[cat_codigo]['previsao_atualizada'] += row[5] or 0
            dados_hierarquicos[cat_codigo]['receita_atual'] += row[6] or 0
            dados_hierarquicos[cat_codigo]['receita_anterior'] += row[7] or 0
            
            # Adiciona subcategoria (fonte)
            dados_hierarquicos[cat_codigo]['subcategorias'].append({
                'codigo': fonte_codigo,
                'descricao': fonte_nome,
                'nivel': 2,
                'previsao_inicial': row[4] or 0,
                'previsao_atualizada': row[5] or 0,
                'receita_atual': row[6] or 0,
                'receita_anterior': row[7] or 0
            })
        
        return list(dados_hierarquicos.values())
    
    def _query_colunas_calculadas(self, estrutura: Dict) -> str:
        """Query para bancos com colunas pré-calculadas"""
        return f"""
        WITH dados_agregados AS (
            SELECT 
                CATEGORIARECEITA,
                COALESCE(NOCATEGORIARECEITA, 'Categoria ' || CATEGORIARECEITA) as nome_categoria,
                COFONTERECEITA,
                COALESCE(NOFONTERECEITA, 'Fonte ' || COFONTERECEITA) as nome_fonte,
                COEXERCICIO,
                INMES,
                SUM(COALESCE("PREVISAO INICIAL", 0)) as previsao_inicial,
                SUM(COALESCE("PREVISAO ATUALIZADA LIQUIDA", "PREVISAO ATUALIZADA", 0)) as previsao_atualizada,
                SUM(COALESCE("RECEITA LIQUIDA", 0)) as receita_liquida
            FROM fato_saldos
            GROUP BY CATEGORIARECEITA, NOCATEGORIARECEITA, 
                     COFONTERECEITA, NOFONTERECEITA,
                     COEXERCICIO, INMES
        )
        SELECT 
            CATEGORIARECEITA,
            nome_categoria,
            COFONTERECEITA,
            nome_fonte,
            SUM(CASE WHEN COEXERCICIO = {self.periodo['ano']} THEN previsao_inicial ELSE 0 END) as previsao_inicial,
            SUM(CASE WHEN COEXERCICIO = {self.periodo['ano']} THEN previsao_atualizada ELSE 0 END) as previsao_atualizada,
            SUM(CASE WHEN COEXERCICIO = {self.periodo['ano']} AND INMES <= {self.periodo['mes']} THEN receita_liquida ELSE 0 END) as receita_atual,
            SUM(CASE WHEN COEXERCICIO = {self.periodo['ano']-1} AND INMES <= {self.periodo['mes']} THEN receita_liquida ELSE 0 END) as receita_anterior
        FROM dados_agregados
        WHERE COEXERCICIO IN ({self.periodo['ano']}, {self.periodo['ano']-1})
        GROUP BY CATEGORIARECEITA, nome_categoria, COFONTERECEITA, nome_fonte
        HAVING (previsao_inicial + previsao_atualizada + receita_atual + receita_anterior) > 0
        ORDER BY CATEGORIARECEITA, COFONTERECEITA
        """
    
    def _query_contas_contabeis(self, estrutura: Dict) -> str:
        """Query para bancos com estrutura de contas contábeis"""
        
        # Define campo de valor e joins
        campo_valor = 'saldo_contabil' if 'saldo_contabil' in estrutura['colunas_fato'] else 'VALANCAMENTO'
        
        if estrutura['tem_dimensoes']:
            # Usa prefixo 'dimensoes.' para acessar tabelas do banco anexado
            joins = """
            LEFT JOIN dimensoes.categorias cat ON l.CATEGORIARECEITA = cat.COCATEGORIARECEITA
            LEFT JOIN dimensoes.origens ori ON l.COFONTERECEITA = ori.COFONTERECEITA
            """
            nome_categoria = "COALESCE(cat.NOCATEGORIARECEITA, 'Categoria ' || l.CATEGORIARECEITA)"
            nome_fonte = "COALESCE(ori.NOFONTERECEITA, 'Fonte ' || l.COFONTERECEITA)"
        else:
            joins = ""
            nome_categoria = "'Categoria ' || l.CATEGORIARECEITA"
            nome_fonte = "'Fonte ' || l.COFONTERECEITA"
        
        return f"""
        SELECT 
            l.CATEGORIARECEITA,
            {nome_categoria} as nome_categoria,
            l.COFONTERECEITA,
            {nome_fonte} as nome_fonte,
            
            -- Previsão Inicial Líquida
            SUM(CASE 
                WHEN l.COEXERCICIO = {self.periodo['ano']} 
                AND {get_filtro_conta('PREVISAO_INICIAL_LIQUIDA')} 
                THEN l.{campo_valor} ELSE 0 
            END) as previsao_inicial,
            
            -- Previsão Atualizada Líquida
            SUM(CASE 
                WHEN l.COEXERCICIO = {self.periodo['ano']} 
                AND {get_filtro_conta('PREVISAO_ATUALIZADA_LIQUIDA')} 
                THEN l.{campo_valor} ELSE 0 
            END) as previsao_atualizada,
            
            -- Receita Realizada Atual
            SUM(CASE 
                WHEN l.COEXERCICIO = {self.periodo['ano']} 
                AND l.INMES <= {self.periodo['mes']}
                AND {get_filtro_conta('RECEITA_LIQUIDA')} 
                THEN l.{campo_valor} ELSE 0 
            END) as receita_atual,
            
            -- Receita Realizada Anterior
            SUM(CASE 
                WHEN l.COEXERCICIO = {self.periodo['ano'] - 1} 
                AND l.INMES <= {self.periodo['mes']}
                AND {get_filtro_conta('RECEITA_LIQUIDA')} 
                THEN l.{campo_valor} ELSE 0 
            END) as receita_anterior
            
        FROM fato_saldos l
        {joins}
        GROUP BY l.CATEGORIARECEITA, nome_categoria, 
                 l.COFONTERECEITA, nome_fonte
        HAVING (previsao_inicial + previsao_atualizada + receita_atual + receita_anterior) > 0
        ORDER BY l.CATEGORIARECEITA, l.COFONTERECEITA
        """
    
    def _calcular_totais(self, dados: List[Dict]) -> Dict:
        """Calcula os totais gerais"""
        totais = {
            'previsao_inicial': 0,
            'previsao_atualizada': 0,
            'receita_atual': 0,
            'receita_anterior': 0
        }
        
        for categoria in dados:
            totais['previsao_inicial'] += categoria['previsao_inicial']
            totais['previsao_atualizada'] += categoria['previsao_atualizada']
            totais['receita_atual'] += categoria['receita_atual']
            totais['receita_anterior'] += categoria['receita_anterior']
        
        # Calcula variações
        if totais['receita_anterior'] > 0:
            totais['variacao_absoluta'] = totais['receita_atual'] - totais['receita_anterior']
            totais['variacao_percentual'] = (totais['variacao_absoluta'] / totais['receita_anterior']) * 100
        else:
            totais['variacao_absoluta'] = totais['receita_atual']
            totais['variacao_percentual'] = 0
        
        return totais
    
    def gerar_html(self, dados_relatorio: Dict) -> str:
        """
        Gera o HTML do relatório com os dados consolidados
        
        Args:
            dados_relatorio: Dados retornados por executar_relatorio()
        """
        
        # Gera linhas da tabela
        linhas_tabela = []
        for categoria in dados_relatorio['dados']:
            # Calcula variação da categoria
            var_absoluta = categoria['receita_atual'] - categoria['receita_anterior']
            var_percentual = (var_absoluta / categoria['receita_anterior'] * 100) if categoria['receita_anterior'] != 0 else 0
            
            # Linha da categoria (nível 1)
            linhas_tabela.append(f'''
                <tr class="nivel-1">
                    <td>{categoria['descricao']}</td>
                    <td>{formatar_moeda(categoria['previsao_inicial'])}</td>
                    <td>{formatar_moeda(categoria['previsao_atualizada'])}</td>
                    <td>{formatar_moeda(categoria['receita_atual'])}</td>
                    <td>{formatar_moeda(categoria['receita_anterior'])}</td>
                    <td>
                        <div class="variacao-cells">
                            <span class="variacao-valor {'valor-positivo' if var_absoluta >= 0 else 'valor-negativo'}">
                                {formatar_moeda(var_absoluta)}
                            </span>
                            <span class="variacao-percentual {'valor-positivo' if var_percentual >= 0 else 'valor-negativo'}">
                                {formatar_percentual(var_percentual/100, casas_decimais=2)}
                            </span>
                        </div>
                    </td>
                </tr>
            ''')
            
            # Linhas das subcategorias (nível 2)
            for sub in categoria['subcategorias']:
                var_sub_absoluta = sub['receita_atual'] - sub['receita_anterior']
                var_sub_percentual = (var_sub_absoluta / sub['receita_anterior'] * 100) if sub['receita_anterior'] != 0 else 0
                
                linhas_tabela.append(f'''
                    <tr class="nivel-2">
                        <td>{sub['descricao']}</td>
                        <td>{formatar_moeda(sub['previsao_inicial'])}</td>
                        <td>{formatar_moeda(sub['previsao_atualizada'])}</td>
                        <td>{formatar_moeda(sub['receita_atual'])}</td>
                        <td>{formatar_moeda(sub['receita_anterior'])}</td>
                        <td>
                            <div class="variacao-cells">
                                <span class="variacao-valor {'valor-positivo' if var_sub_absoluta >= 0 else 'valor-negativo'}">
                                    {formatar_moeda(var_sub_absoluta)}
                                </span>
                                <span class="variacao-percentual {'valor-positivo' if var_sub_percentual >= 0 else 'valor-negativo'}">
                                    {formatar_percentual(var_sub_percentual/100, casas_decimais=2)}
                                </span>
                            </div>
                        </td>
                    </tr>
                ''')
        
        # Linha de total geral
        totais = dados_relatorio['totais']
        linhas_tabela.append(f'''
            <tr class="total-geral">
                <td>TOTAL GERAL</td>
                <td>{formatar_moeda(totais['previsao_inicial'])}</td>
                <td>{formatar_moeda(totais['previsao_atualizada'])}</td>
                <td>{formatar_moeda(totais['receita_atual'])}</td>
                <td>{formatar_moeda(totais['receita_anterior'])}</td>
                <td>
                    <div class="variacao-cells">
                        <span class="variacao-valor {'valor-positivo' if totais['variacao_absoluta'] >= 0 else 'valor-negativo'}">
                            {formatar_moeda(totais['variacao_absoluta'])}
                        </span>
                        <span class="variacao-percentual {'valor-positivo' if totais['variacao_percentual'] >= 0 else 'valor-negativo'}">
                            {formatar_percentual(totais['variacao_percentual']/100, casas_decimais=2)}
                        </span>
                    </div>
                </td>
            </tr>
        ''')
        
        # Template HTML
        html = f'''<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Balanço Orçamentário da Receita - Consolidado</title>
    <style>
        {self._get_css()}
    </style>
</head>
<body>
    <div class="container">
        <!-- Header -->
        <div class="header">
            <h1>Balanço Orçamentário da Receita</h1>
            <p>Demonstrativo consolidado comparativo entre a previsão e a realização das receitas</p>
        </div>

        <!-- Info do Período -->
        <div class="periodo-info">
            <div>
                <span><strong>Período de Referência:</strong> {dados_relatorio['periodo']['periodo_completo']}</span>
                <span class="separator">|</span>
                <span><strong>Data de Geração:</strong> {dados_relatorio['data_geracao']}</span>
            </div>
            <div class="info-consolidado">
                <span class="badge">DADOS CONSOLIDADOS</span>
            </div>
        </div>

        <!-- Tabela -->
        <div class="table-container">
            <table>
                <thead>
                    <tr>
                        <th>RECEITAS</th>
                        <th>PREVISÃO INICIAL<br>{dados_relatorio['periodo']['ano']}</th>
                        <th>PREVISÃO ATUALIZADA<br>{dados_relatorio['periodo']['ano']}</th>
                        <th>RECEITA REALIZADA<br>{dados_relatorio['periodo']['mes']:02d}/{dados_relatorio['periodo']['ano']}</th>
                        <th>RECEITA REALIZADA<br>{dados_relatorio['periodo']['mes']:02d}/{dados_relatorio['periodo']['ano']-1}</th>
                        <th>
                            <div class="variacao-header">
                                <span>VARIAÇÃO {dados_relatorio['periodo']['ano']} X {dados_relatorio['periodo']['ano']-1}</span>
                                <div class="variacao-subheader">
                                    <span>VALOR ABSOLUTO</span>
                                    <span>%</span>
                                </div>
                            </div>
                        </th>
                    </tr>
                </thead>
                <tbody>
                    {''.join(linhas_tabela)}
                </tbody>
            </table>
        </div>

        <!-- Rodapé com botões de ação -->
        <div class="footer-actions">
            <button class="btn btn-print" onclick="window.print()">
                📄 Imprimir Relatório
            </button>
            <button class="btn btn-excel" onclick="alert('Função de exportação para Excel')">
                📊 Exportar para Excel
            </button>
        </div>
    </div>
</body>
</html>'''
        
        return html
    
    def _get_css(self) -> str:
        """Retorna o CSS do relatório"""
        return '''
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            background-color: #f5f7fa;
            color: #2c3e50;
            line-height: 1.6;
        }

        .container {
            max-width: 1400px;
            margin: 0 auto;
            padding: 20px;
        }

        /* Header */
        .header {
            background: linear-gradient(135deg, #1e3c72 0%, #2a5298 100%);
            color: white;
            padding: 40px;
            border-radius: 10px 10px 0 0;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            text-align: center;
        }

        .header h1 {
            font-size: 32px;
            font-weight: 600;
            margin-bottom: 10px;
            letter-spacing: -0.5px;
        }

        .header p {
            font-size: 16px;
            opacity: 0.9;
        }

        /* Info período */
        .periodo-info {
            background: white;
            padding: 20px 30px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            border-left: 4px solid #2a5298;
            box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
        }

        .periodo-info span {
            font-size: 14px;
            color: #5a6c7d;
        }

        .periodo-info strong {
            color: #1e3c72;
            font-weight: 600;
        }

        .separator {
            margin: 0 15px;
            color: #cbd5e0;
        }

        .info-consolidado {
            display: flex;
            align-items: center;
        }

        .badge {
            background: #2a5298;
            color: white;
            padding: 6px 16px;
            border-radius: 20px;
            font-size: 12px;
            font-weight: 600;
            letter-spacing: 0.5px;
        }

        /* Tabela */
        .table-container {
            background: white;
            overflow: auto;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        }

        table {
            width: 100%;
            border-collapse: collapse;
            min-width: 1000px;
        }

        thead {
            background: #1e3c72;
            color: white;
            position: sticky;
            top: 0;
            z-index: 10;
        }

        thead th {
            padding: 18px 15px;
            text-align: right;
            font-weight: 600;
            font-size: 13px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            border-right: 1px solid rgba(255, 255, 255, 0.1);
        }

        thead th:first-child {
            text-align: left;
            min-width: 350px;
        }

        thead th:last-child {
            border-right: none;
            min-width: 250px;
        }

        tbody tr {
            border-bottom: 1px solid #e8ecf0;
            transition: background-color 0.2s;
        }

        tbody tr:hover {
            background-color: #f8fafc;
        }

        tbody td {
            padding: 14px 15px;
            text-align: right;
            font-size: 14px;
        }

        tbody td:first-child {
            text-align: left;
            font-weight: 500;
        }

        /* Níveis */
        .nivel-1 {
            background-color: #f0f4f8;
            font-weight: 600;
        }

        .nivel-1 td:first-child {
            padding-left: 25px;
            color: #1e3c72;
            font-size: 15px;
        }

        .nivel-2 td:first-child {
            padding-left: 55px;
            color: #5a6c7d;
            font-size: 13px;
            font-weight: 400;
        }

        /* Total Geral */
        .total-geral {
            background: linear-gradient(135deg, #1e3c72 0%, #2a5298 100%);
            color: white;
            font-weight: 700;
        }

        .total-geral td {
            padding: 20px 15px;
            font-size: 16px;
            border-top: 2px solid #1e3c72;
        }

        /* Valores */
        .valor-positivo {
            color: #27ae60;
            font-weight: 600;
        }

        .valor-negativo {
            color: #e74c3c;
            font-weight: 600;
        }

        /* Variação */
        .variacao-header {
            display: flex;
            flex-direction: column;
            align-items: center;
            gap: 5px;
        }

        .variacao-subheader {
            display: flex;
            width: 100%;
            border-top: 1px solid rgba(255, 255, 255, 0.2);
            margin-top: 5px;
            padding-top: 5px;
        }

        .variacao-subheader span {
            flex: 1;
            text-align: center;
            font-size: 11px;
            font-weight: 500;
            opacity: 0.9;
        }

        .variacao-cells {
            display: flex;
            gap: 20px;
            justify-content: flex-end;
            align-items: center;
        }

        .variacao-valor {
            min-width: 120px;
            text-align: right;
        }

        .variacao-percentual {
            min-width: 60px;
            text-align: right;
        }

        /* Rodapé com ações */
        .footer-actions {
            background: white;
            padding: 20px 30px;
            border-radius: 0 0 10px 10px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            display: flex;
            justify-content: center;
            gap: 15px;
            margin-bottom: 40px;
        }

        .btn {
            padding: 12px 24px;
            border-radius: 6px;
            font-size: 14px;
            font-weight: 500;
            cursor: pointer;
            transition: all 0.2s;
            border: none;
            display: flex;
            align-items: center;
            gap: 8px;
        }

        .btn-print {
            background: #2a5298;
            color: white;
        }

        .btn-print:hover {
            background: #1e3c72;
            transform: translateY(-1px);
            box-shadow: 0 4px 12px rgba(42, 82, 152, 0.3);
        }

        .btn-excel {
            background: #27ae60;
            color: white;
        }

        .btn-excel:hover {
            background: #219a52;
            transform: translateY(-1px);
            box-shadow: 0 4px 12px rgba(39, 174, 96, 0.3);
        }

        /* Print styles */
        @media print {
            body {
                background: white;
            }
            
            .container {
                max-width: 100%;
                padding: 0;
            }
            
            .header {
                background: none;
                color: black;
                border-bottom: 2px solid #333;
                padding: 20px;
            }
            
            .footer-actions {
                display: none;
            }
            
            .periodo-info {
                border-left-color: #333;
            }
            
            .badge {
                background: #333;
                -webkit-print-color-adjust: exact;
                print-color-adjust: exact;
            }
            
            thead {
                background: #333;
                -webkit-print-color-adjust: exact;
                print-color-adjust: exact;
            }
            
            .nivel-1 {
                background-color: #f0f0f0 !important;
                -webkit-print-color-adjust: exact;
                print-color-adjust: exact;
            }
            
            .total-geral {
                background: #333 !important;
                -webkit-print-color-adjust: exact;
                print-color-adjust: exact;
            }
            
            table {
                font-size: 11px;
            }
        }
        '''


# Exemplo de uso
if __name__ == "__main__":
    # Caminho do banco de dados
    caminho_db = os.path.join(os.path.dirname(__file__), '../../dados/db/banco_saldo_receita.db')
    
    # Cria instância do relatório
    relatorio = BalancoOrcamentarioReceita(caminho_db)
    
    # Executa o relatório consolidado
    dados = relatorio.executar_relatorio()
    
    # Gera HTML
    html = relatorio.gerar_html(dados)
    
    # Salva em arquivo
    with open('balanco_orcamentario_receita_consolidado.html', 'w', encoding='utf-8') as f:
        f.write(html)
    
    print("Relatório consolidado gerado com sucesso!")
    print(f"Período: {dados['periodo']['periodo_completo']}")
    print(f"Total de categorias: {len(dados['dados'])}")
    print(f"Receita Total Atual: {formatar_moeda(dados['totais']['receita_atual'])}")
    print(f"Variação: {formatar_percentual(dados['totais']['variacao_percentual']/100)}")