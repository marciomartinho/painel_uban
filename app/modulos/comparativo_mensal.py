# app/modulos/comparativo_mensal.py
"""
Módulo para gerar o comparativo mensal acumulado de receitas
Mostra a evolução acumulada mês a mês com variações percentuais
"""

import sqlite3
from typing import List, Dict, Optional

# Importações corrigidas e centralizadas
from app.modulos.formatacao import formatar_moeda, formatar_percentual
from app.modulos.regras_contabeis_receita import get_filtro_conta, FILTROS_RELATORIO_ESPECIAIS
from app.modulos.conexao_hibrida import adaptar_query


class ComparativoMensalAcumulado:
    """Classe para gerar dados do comparativo mensal acumulado"""
    
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        
    def gerar_comparativo(self, ano: int, coug: Optional[str] = None, 
                         filtro_relatorio_key: Optional[str] = None) -> List[Dict]:
        """
        Gera o comparativo mensal acumulado de forma compatível e robusta.
        """
        filtros_sql = []
        if coug:
            coug_escaped = coug.replace("'", "''")
            filtros_sql.append(f"fs.coug = '{coug_escaped}'")
        
        if filtro_relatorio_key and filtro_relatorio_key in FILTROS_RELATORIO_ESPECIAIS:
            regra = FILTROS_RELATORIO_ESPECIAIS[filtro_relatorio_key]
            campo = regra['campo_filtro'].lower()
            valores_str = ", ".join([f"'{v}'" for v in regra['valores']])
            filtros_sql.append(f"fs.{campo} IN ({valores_str})")
        
        where_clause = " AND " + " AND ".join(filtros_sql) if filtros_sql else ""
        
        # --- CORREÇÃO APLICADA AQUI ---
        # Removido o prefixo "dim_tempo." duplicado. Agora é "FROM dim_tempo"
        query_original = f"""
        WITH meses AS (
            SELECT DISTINCT inmes, nome_mes FROM dim_tempo WHERE coexercicio = {ano}
        ),
        receitas_mensais AS (
            SELECT
                coexercicio,
                inmes,
                SUM(saldo_contabil) as receita_liquida
            FROM fato_saldos fs
            WHERE
                coexercicio IN ({ano}, {ano - 1})
                AND {get_filtro_conta('RECEITA_LIQUIDA').lower()}
                {where_clause}
            GROUP BY coexercicio, inmes
        )
        SELECT
            m.inmes,
            m.nome_mes,
            (SELECT SUM(r.receita_liquida) FROM receitas_mensais r WHERE r.coexercicio = {ano} AND r.inmes <= m.inmes) as receita_atual,
            (SELECT SUM(r.receita_liquida) FROM receitas_mensais r WHERE r.coexercicio = {ano - 1} AND r.inmes <= m.inmes) as receita_anterior
        FROM meses m
        ORDER BY m.inmes
        """
        
        query_adaptada = adaptar_query(query_original)
        
        cursor = self.conn.cursor()
        cursor.execute(query_adaptada)
        
        resultados = []
        for row_dict in cursor.fetchall():
            row = {str(k).lower(): v for k, v in dict(row_dict).items()}
            
            receita_atual = row.get('receita_atual') or 0
            receita_anterior = row.get('receita_anterior') or 0

            if receita_atual != 0 or receita_anterior != 0:
                variacao_absoluta = receita_atual - receita_anterior
                if receita_anterior != 0:
                    variacao_percentual = (variacao_absoluta / abs(receita_anterior)) * 100
                else:
                    variacao_percentual = 100.0 if receita_atual != 0 else 0.0

                resultados.append({
                    'mes': row['inmes'],
                    'nome_mes': row['nome_mes'],
                    'ano_atual': ano,
                    'ano_anterior': ano - 1,
                    'receita_atual': receita_atual,
                    'receita_anterior': receita_anterior,
                    'variacao_absoluta': variacao_absoluta,
                    'variacao_percentual': variacao_percentual
                })
        
        return resultados
    
    def formatar_para_html(self, dados: List[Dict]) -> Dict:
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
                'variacao_formatada': formatar_percentual(item['variacao_percentual'] / 100, casas_decimais=2, usar_cor=True, html=True),
                'variacao_classe': 'positiva' if item['variacao_percentual'] >= 0 else 'negativa',
                'variacao_percentual': item['variacao_percentual']
            })
        
        return {'meses': meses_formatados, 'tem_dados': True}
    
    def gerar_dados_grafico(self, dados: List[Dict]) -> Dict:
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


def gerar_comparativo_mensal(conn: sqlite3.Connection, ano: int, 
                            coug: Optional[str] = None, 
                            filtro_relatorio_key: Optional[str] = None) -> Dict:
    """
    Função auxiliar para gerar o comparativo completo
    """
    comparativo = ComparativoMensalAcumulado(conn)
    dados = comparativo.gerar_comparativo(ano, coug, filtro_relatorio_key)
    dados_html = comparativo.formatar_para_html(dados)
    dados_grafico = comparativo.gerar_dados_grafico(dados)
    
    return {
        'dados_html': dados_html,
        'dados_grafico': dados_grafico,
        'dados_brutos': dados
    }