# app/modulos/comparativo_mensal.py
"""
Módulo para gerar o comparativo mensal acumulado de receitas
Mostra a evolução acumulada mês a mês com variações percentuais
"""

import sqlite3
from typing import List, Dict, Optional
from app.modulos.formatacao import formatar_moeda, formatar_percentual
from app.modulos.regras_contabeis_receita import get_filtro_conta, FILTROS_RELATORIO_ESPECIAIS


class ComparativoMensalAcumulado:
    """Classe para gerar dados do comparativo mensal acumulado"""
    
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        
    def gerar_comparativo(self, ano: int, coug: Optional[str] = None, 
                         filtro_relatorio_key: Optional[str] = None) -> List[Dict]:
        """
        Gera o comparativo mensal acumulado
        
        Args:
            ano: Ano de referência
            coug: Código da COUG (opcional)
            filtro_relatorio_key: Chave do filtro especial (opcional)
            
        Returns:
            Lista com dados mensais acumulados
        """
        # Monta os filtros
        filtros = []
        
        # Filtro de COUG
        if coug:
            coug_escaped = coug.replace("'", "''")
            filtros.append(f"fs.COUG = '{coug_escaped}'")
        
        # Filtro dinâmico de tipo de receita
        if filtro_relatorio_key and filtro_relatorio_key in FILTROS_RELATORIO_ESPECIAIS:
            regra = FILTROS_RELATORIO_ESPECIAIS[filtro_relatorio_key]
            campo = regra['campo_filtro']
            valores_str = ", ".join([f"'{v}'" for v in regra['valores']])
            filtros.append(f"fs.{campo} IN ({valores_str})")
        
        # Monta a cláusula WHERE
        where_clause = " AND ".join(filtros) if filtros else "1=1"
        
        # Query para buscar dados acumulados por mês
        query = f"""
        WITH meses_disponiveis AS (
            SELECT DISTINCT 
                INMES,
                CASE INMES
                    WHEN 1 THEN 'Janeiro'
                    WHEN 2 THEN 'Fevereiro'
                    WHEN 3 THEN 'Março'
                    WHEN 4 THEN 'Abril'
                    WHEN 5 THEN 'Maio'
                    WHEN 6 THEN 'Junho'
                    WHEN 7 THEN 'Julho'
                    WHEN 8 THEN 'Agosto'
                    WHEN 9 THEN 'Setembro'
                    WHEN 10 THEN 'Outubro'
                    WHEN 11 THEN 'Novembro'
                    WHEN 12 THEN 'Dezembro'
                END as nome_mes
            FROM fato_saldos
            WHERE COEXERCICIO IN ({ano}, {ano-1})
            ORDER BY INMES
        ),
        dados_acumulados AS (
            SELECT 
                m.INMES,
                m.nome_mes,
                -- Valores do ano atual
                SUM(CASE 
                    WHEN fs.COEXERCICIO = {ano} 
                    AND fs.INMES <= m.INMES
                    AND {get_filtro_conta('RECEITA_LIQUIDA')}
                    AND {where_clause}
                    THEN fs.saldo_contabil 
                    ELSE 0 
                END) as receita_atual,
                -- Valores do ano anterior
                SUM(CASE 
                    WHEN fs.COEXERCICIO = {ano-1} 
                    AND fs.INMES <= m.INMES
                    AND {get_filtro_conta('RECEITA_LIQUIDA')}
                    AND {where_clause}
                    THEN fs.saldo_contabil 
                    ELSE 0 
                END) as receita_anterior
            FROM meses_disponiveis m
            CROSS JOIN fato_saldos fs
            GROUP BY m.INMES, m.nome_mes
            HAVING receita_atual != 0 OR receita_anterior != 0
        )
        SELECT 
            INMES,
            nome_mes,
            receita_atual,
            receita_anterior,
            (receita_atual - receita_anterior) as variacao_absoluta,
            CASE 
                WHEN receita_anterior != 0 
                THEN ((receita_atual - receita_anterior) / ABS(receita_anterior)) * 100
                ELSE 0
            END as variacao_percentual
        FROM dados_acumulados
        ORDER BY INMES
        """
        
        cursor = self.conn.execute(query)
        resultados = []
        
        for row in cursor:
            resultados.append({
                'mes': row['INMES'],
                'nome_mes': row['nome_mes'],
                'ano_atual': ano,
                'ano_anterior': ano - 1,
                'receita_atual': row['receita_atual'] or 0,
                'receita_anterior': row['receita_anterior'] or 0,
                'variacao_absoluta': row['variacao_absoluta'] or 0,
                'variacao_percentual': row['variacao_percentual'] or 0
            })
        
        return resultados
    
    def formatar_para_html(self, dados: List[Dict]) -> Dict:
        """
        Formata os dados para exibição em HTML
        
        Args:
            dados: Lista de dados mensais
            
        Returns:
            Dicionário com dados formatados
        """
        if not dados:
            return {'meses': [], 'tem_dados': False}
        
        meses_formatados = []
        
        for item in dados:
            meses_formatados.append({
                'nome_mes': item['nome_mes'],
                'label_ate': f"Total até\n{item['nome_mes']}",
                'ano_atual': item['ano_atual'],
                'ano_anterior': item['ano_anterior'],
                'receita_atual_formatada': formatar_moeda(item['receita_atual']),
                'receita_anterior_formatada': formatar_moeda(item['receita_anterior']),
                'variacao_formatada': formatar_percentual(
                    item['variacao_percentual'] / 100,
                    casas_decimais=2,
                    usar_cor=True,
                    html=True
                ),
                'variacao_classe': 'positiva' if item['variacao_percentual'] >= 0 else 'negativa',
                'variacao_percentual': item['variacao_percentual']
            })
        
        return {
            'meses': meses_formatados,
            'tem_dados': True
        }
    
    def gerar_dados_grafico(self, dados: List[Dict]) -> Dict:
        """
        Gera dados formatados para gráfico Chart.js
        
        Args:
            dados: Lista de dados mensais
            
        Returns:
            Dicionário com dados para o gráfico
        """
        if not dados:
            return {'labels': [], 'datasets': []}
        
        labels = [item['nome_mes'] for item in dados]
        valores_atuais = [item['receita_atual'] for item in dados]
        valores_anteriores = [item['receita_anterior'] for item in dados]
        
        ano_atual = dados[0]['ano_atual'] if dados else 2025
        ano_anterior = dados[0]['ano_anterior'] if dados else 2024
        
        return {
            'labels': labels,
            'datasets': [
                {
                    'label': str(ano_anterior),
                    'data': valores_anteriores,
                    'borderColor': '#95a5a6',
                    'backgroundColor': 'rgba(149, 165, 166, 0.1)',
                    'borderWidth': 2,
                    'tension': 0.1
                },
                {
                    'label': str(ano_atual),
                    'data': valores_atuais,
                    'borderColor': '#2a5298',
                    'backgroundColor': 'rgba(42, 82, 152, 0.1)',
                    'borderWidth': 3,
                    'tension': 0.1
                }
            ]
        }


# Funções auxiliares para facilitar o uso

def gerar_comparativo_mensal(conn: sqlite3.Connection, ano: int, 
                            coug: Optional[str] = None, 
                            filtro_relatorio_key: Optional[str] = None) -> Dict:
    """
    Função auxiliar para gerar o comparativo completo
    
    Returns:
        Dicionário com dados formatados e dados para gráfico
    """
    comparativo = ComparativoMensalAcumulado(conn)
    
    # Gera os dados
    dados = comparativo.gerar_comparativo(ano, coug, filtro_relatorio_key)
    
    # Formata para HTML
    dados_html = comparativo.formatar_para_html(dados)
    
    # Gera dados para gráfico
    dados_grafico = comparativo.gerar_dados_grafico(dados)
    
    return {
        'dados_html': dados_html,
        'dados_grafico': dados_grafico,
        'dados_brutos': dados
    }