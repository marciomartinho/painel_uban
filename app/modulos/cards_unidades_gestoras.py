# app/modulos/cards_unidades_gestoras.py
"""
Módulo para exibir cards com as Unidades Gestoras que possuem receita realizada
Dinâmico com base nos filtros de tipo de receita selecionados
"""

import sqlite3
from typing import List, Dict, Optional
# --- ADIÇÕES PARA A CORREÇÃO ---
import psycopg2.extras
from app.modulos.conexao_hibrida import get_db_environment, adaptar_query
# --- FIM DAS ADIÇÕES ---
from app.modulos.formatacao import formatar_moeda
from app.modulos.regras_contabeis_receita import get_filtro_conta, FILTROS_RELATORIO_ESPECIAIS


class CardsUnidadesGestoras:
    """Classe para gerar cards de unidades gestoras com receita realizada"""

    def __init__(self, conn):
        self.conn = conn
        # --- CÓDIGO CORRIGIDO ---
        # Cria um cursor que se comporta da mesma forma nos dois bancos, retornando linhas que podem ser acessadas por nome de coluna.
        if get_db_environment() == 'postgres':
            self.cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        else:
            # O row_factory já foi definido na conexão para SQLite
            self.cursor = conn.cursor()
        # --- FIM DA CORREÇÃO ---

    def buscar_unidades_com_receita(self, ano: int, mes: int,
                                   filtro_relatorio_key: Optional[str] = None) -> List[Dict]:
        """
        Busca todas as unidades gestoras que possuem receita realizada
        """
        filtro_dinamico = ""
        if filtro_relatorio_key and filtro_relatorio_key in FILTROS_RELATORIO_ESPECIAIS:
            regra = FILTROS_RELATORIO_ESPECIAIS[filtro_relatorio_key]
            campo = regra['campo_filtro']
            valores_str = ", ".join([f"'{v}'" for v in regra['valores']])
            filtro_dinamico = f"AND fs.{campo} IN ({valores_str})"

        # --- QUERY CORRIGIDA COM COLUNAS EM MINÚSCULAS ---
        query = f"""
        WITH receitas_por_ug AS (
            SELECT
                fs.coug,
                COALESCE(ug.noug, 'UG ' || fs.coug) as noug,
                SUM(CASE
                    WHEN fs.coexercicio = {ano}
                    AND fs.inmes <= {mes}
                    AND {get_filtro_conta('RECEITA_LIQUIDA')}
                    {filtro_dinamico}
                    THEN fs.saldo_contabil
                    ELSE 0
                END) as receita_realizada,
                SUM(CASE
                    WHEN fs.coexercicio = {ano-1}
                    AND fs.inmes <= {mes}
                    AND {get_filtro_conta('RECEITA_LIQUIDA')}
                    {filtro_dinamico}
                    THEN fs.saldo_contabil
                    ELSE 0
                END) as receita_anterior
            FROM fato_saldos fs
            LEFT JOIN dimensoes.unidades_gestoras ug ON fs.coug = ug.coug
            WHERE fs.coug IS NOT NULL
              AND fs.coug != ''
            GROUP BY fs.coug, ug.noug
            HAVING receita_realizada > 0
        )
        SELECT
            coug,
            noug,
            receita_realizada,
            receita_anterior,
            CASE
                WHEN receita_anterior > 0
                THEN ((receita_realizada - receita_anterior) / receita_anterior) * 100
                ELSE 100.0
            END as variacao_percentual,
            (receita_realizada - receita_anterior) as variacao_absoluta
        FROM receitas_por_ug
        ORDER BY receita_realizada DESC
        """

        # --- CÓDIGO CORRIGIDO ---
        query_adaptada = adaptar_query(query)
        self.cursor.execute(query_adaptada) # Usa o cursor criado no __init__
        unidades = []

        for row in self.cursor: # Agora a 'row' será dict-like nos dois bancos
            unidades.append({
                'codigo': row['coug'],
                'nome': row['noug'],
                'descricao_completa': f"{row['coug']} - {row['noug']}",
                'receita_realizada': row['receita_realizada'] or 0,
                'receita_anterior': row['receita_anterior'] or 0,
                'variacao_percentual': row['variacao_percentual'] or 0,
                'variacao_absoluta': row['variacao_absoluta'] or 0
            })
        return unidades

    def agrupar_por_faixa_valor(self, unidades: List[Dict]) -> Dict[str, List[Dict]]:
        """
        Agrupa unidades por faixas de valor de receita
        """
        faixas = {
            'grandes': {'min': 100000000, 'max': float('inf'), 'label': 'Grandes (> R$ 100M)', 'unidades': []},
            'medias': {'min': 10000000, 'max': 100000000, 'label': 'Médias (R$ 10M - R$ 100M)', 'unidades': []},
            'pequenas': {'min': 1000000, 'max': 10000000, 'label': 'Pequenas (R$ 1M - R$ 10M)', 'unidades': []},
            'micro': {'min': 0, 'max': 1000000, 'label': 'Micro (< R$ 1M)', 'unidades': []}
        }

        for unidade in unidades:
            valor = unidade['receita_realizada']

            if valor >= faixas['grandes']['min']:
                faixas['grandes']['unidades'].append(unidade)
            elif valor >= faixas['medias']['min']:
                faixas['medias']['unidades'].append(unidade)
            elif valor >= faixas['pequenas']['min']:
                faixas['pequenas']['unidades'].append(unidade)
            else:
                faixas['micro']['unidades'].append(unidade)

        return faixas

    def calcular_totais(self, unidades: List[Dict]) -> Dict:
        """
        Calcula totais e estatísticas das unidades
        """
        if not unidades:
            return {
                'total_unidades': 0, 'receita_total': 0, 'receita_total_anterior': 0,
                'variacao_total_absoluta': 0, 'variacao_total_percentual': 0,
                'maior_receita': None, 'maior_crescimento': None, 'maior_queda': None
            }

        receita_total = sum(u['receita_realizada'] for u in unidades)
        receita_total_anterior = sum(u['receita_anterior'] for u in unidades)

        variacao_absoluta = receita_total - receita_total_anterior
        variacao_percentual = (variacao_absoluta / receita_total_anterior * 100) if receita_total_anterior > 0 else 0

        maior_receita = max(unidades, key=lambda u: u['receita_realizada'])
        unidades_com_historico = [u for u in unidades if u['receita_anterior'] > 0]
        maior_crescimento, maior_queda = None, None

        if unidades_com_historico:
            maior_crescimento = max(unidades_com_historico, key=lambda u: u['variacao_percentual'])
            maior_queda = min(unidades_com_historico, key=lambda u: u['variacao_percentual'])
            if maior_crescimento['variacao_percentual'] <= 0: maior_crescimento = None
            if maior_queda['variacao_percentual'] >= 0: maior_queda = None

        return {
            'total_unidades': len(unidades), 'receita_total': receita_total,
            'receita_total_anterior': receita_total_anterior,
            'variacao_total_absoluta': variacao_absoluta, 'variacao_total_percentual': variacao_percentual,
            'maior_receita': maior_receita, 'maior_crescimento': maior_crescimento, 'maior_queda': maior_queda
        }

    def formatar_para_html(self, unidades: List[Dict], totais: Dict) -> Dict:
        """
        Formata os dados para exibição em HTML
        """
        for unidade in unidades:
            unidade['receita_formatada'] = formatar_moeda(unidade['receita_realizada'])
            unidade['receita_anterior_formatada'] = formatar_moeda(unidade['receita_anterior'])
            unidade['variacao_formatada'] = f"{unidade['variacao_percentual']:.2f}%"
            unidade['variacao_classe'] = 'positiva' if unidade['variacao_percentual'] >= 0 else 'negativa'
            if unidade['receita_realizada'] >= 100000000: unidade['icone'] = '🏛️'
            elif unidade['receita_realizada'] >= 10000000: unidade['icone'] = '🏢'
            elif unidade['receita_realizada'] >= 1000000: unidade['icone'] = '🏘️'
            else: unidade['icone'] = '🏠'

        totais_formatados = {
            'total_unidades': totais['total_unidades'],
            'receita_total_formatada': formatar_moeda(totais['receita_total']),
            'receita_total_anterior_formatada': formatar_moeda(totais['receita_total_anterior']),
            'variacao_total_formatada': f"{totais['variacao_total_percentual']:.2f}%",
            'variacao_total_classe': 'positiva' if totais['variacao_total_percentual'] >= 0 else 'negativa'
        }

        if totais['maior_receita']:
            totais_formatados['maior_receita'] = { 'nome': totais['maior_receita']['nome'], 'valor_formatado': formatar_moeda(totais['maior_receita']['receita_realizada']) }
        if totais['maior_crescimento']:
            totais_formatados['maior_crescimento'] = { 'nome': totais['maior_crescimento']['nome'], 'variacao_formatada': f"+{totais['maior_crescimento']['variacao_percentual']:.2f}%" }
        if totais['maior_queda']:
            totais_formatados['maior_queda'] = { 'nome': totais['maior_queda']['nome'], 'variacao_formatada': f"{totais['maior_queda']['variacao_percentual']:.2f}%" }

        return {'unidades': unidades, 'totais': totais_formatados, 'tem_dados': len(unidades) > 0}


# Funções auxiliares para facilitar o uso

def gerar_cards_unidades(conn: sqlite3.Connection, ano: int, mes: int,
                        filtro_relatorio_key: Optional[str] = None) -> Dict:
    """
    Função auxiliar para gerar todos os dados dos cards
    """
    cards = CardsUnidadesGestoras(conn)
    unidades = cards.buscar_unidades_com_receita(ano, mes, filtro_relatorio_key)
    totais = cards.calcular_totais(unidades)
    faixas = cards.agrupar_por_faixa_valor(unidades)
    dados_formatados = cards.formatar_para_html(unidades, totais)

    return {
        'dados_formatados': dados_formatados, 'faixas': faixas,
        'unidades_raw': unidades, 'totais_raw': totais
    }