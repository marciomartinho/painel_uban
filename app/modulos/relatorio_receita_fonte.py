# app/modulos/relatorio_receita_fonte.py
"""
Módulo para gerar relatórios agrupados por Código de Receita ou Código de Fonte
Permite visualização hierárquica com expansão/colapso
"""

import sqlite3
from typing import List, Dict, Optional, Literal
from app.modulos.formatacao import formatar_moeda
from app.modulos.regras_contabeis_receita import get_filtro_conta, FILTROS_RELATORIO_ESPECIAIS
from .conexao_hibrida import adaptar_query # Importar o adaptador


class RelatorioReceitaFonte:
    """Classe para gerar relatórios agrupados por receita ou fonte"""
    
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.estrutura = self._verificar_estrutura()
        
    def _verificar_estrutura(self):
        tem_lancamentos = False
        try:
            query = adaptar_query("SELECT 1 FROM lancamentos_db.lancamentos LIMIT 1")
            cursor = self.conn.execute(query)
            if cursor.fetchone():
                tem_lancamentos = True
        except:
            pass
        return {'tem_lancamentos': tem_lancamentos}
        
    def _gerar_relatorio(self, tipo: Literal['receita', 'fonte'], 
                        ano: int, mes: int, 
                        coug: Optional[str] = None,
                        filtro_relatorio_key: Optional[str] = None) -> List[Dict]:
        
        filtros = []
        if coug:
            coug_escaped = coug.replace("'", "''")
            filtros.append(f"fs.COUG = '{coug_escaped}'")
        
        if filtro_relatorio_key and filtro_relatorio_key in FILTROS_RELATORIO_ESPECIAIS:
            regra = FILTROS_RELATORIO_ESPECIAIS[filtro_relatorio_key]
            campo = regra['campo_filtro']
            valores_str = ", ".join([f"'{v}'" for v in regra['valores']])
            filtros.append(f"fs.{campo} IN ({valores_str})")
        
        where_clause = " AND " + " AND ".join(filtros) if filtros else ""
        
        if tipo == 'receita':
            campo_principal, nome_principal, tabela_principal = 'COALINEA', 'NOALINEA', 'alineas'
            campo_secundario, nome_secundario, tabela_secundaria = 'COFONTE', 'NOFONTE', 'fontes'
        else:
            campo_principal, nome_principal, tabela_principal = 'COFONTE', 'NOFONTE', 'fontes'
            campo_secundario, nome_secundario, tabela_secundaria = 'COALINEA', 'NOALINEA', 'alineas'
        
        query_original = f"""
        WITH dados_agregados AS (
            SELECT 
                fs.{campo_principal},
                COALESCE(dp.{nome_principal}, 'Código ' || fs.{campo_principal}) as nome_principal,
                fs.{campo_secundario},
                COALESCE(ds.{nome_secundario}, 'Código ' || fs.{campo_secundario}) as nome_secundario,
                fs.COEXERCICIO,
                fs.INMES,
                SUM(CASE WHEN {get_filtro_conta('PREVISAO_INICIAL_LIQUIDA')} THEN fs.saldo_contabil ELSE 0 END) as previsao_inicial,
                SUM(CASE WHEN {get_filtro_conta('PREVISAO_ATUALIZADA_LIQUIDA')} THEN fs.saldo_contabil ELSE 0 END) as previsao_atualizada,
                SUM(CASE WHEN {get_filtro_conta('RECEITA_LIQUIDA')} THEN fs.saldo_contabil ELSE 0 END) as receita_liquida
            FROM fato_saldos fs
            LEFT JOIN dimensoes.{tabela_principal} dp ON fs.{campo_principal} = dp.CO{campo_principal.replace('CO', '')}
            LEFT JOIN dimensoes.{tabela_secundaria} ds ON fs.{campo_secundario} = ds.CO{campo_secundario.replace('CO', '')}
            WHERE fs.{campo_principal} IS NOT NULL AND fs.{campo_principal} != '' {where_clause}
            GROUP BY 1, 2, 3, 4, 5, 6
        ),
        dados_sumarizados AS (
            SELECT 
                {campo_principal}, nome_principal, {campo_secundario}, nome_secundario,
                SUM(CASE WHEN COEXERCICIO = {ano} THEN previsao_inicial ELSE 0 END) as previsao_inicial,
                SUM(CASE WHEN COEXERCICIO = {ano} THEN previsao_atualizada ELSE 0 END) as previsao_atualizada,
                SUM(CASE WHEN COEXERCICIO = {ano} AND INMES <= {mes} THEN receita_liquida ELSE 0 END) as receita_atual,
                SUM(CASE WHEN COEXERCICIO = {ano-1} AND INMES <= {mes} THEN receita_liquida ELSE 0 END) as receita_anterior
            FROM dados_agregados WHERE COEXERCICIO IN ({ano}, {ano-1})
            GROUP BY 1, 2, 3, 4
        ),
        totais_principais AS (
            SELECT 
                {campo_principal}, nome_principal,
                SUM(previsao_inicial) as total_previsao_inicial, SUM(previsao_atualizada) as total_previsao_atualizada,
                SUM(receita_atual) as total_receita_atual, SUM(receita_anterior) as total_receita_anterior
            FROM dados_sumarizados GROUP BY 1, 2
        )
        SELECT ds.*, tp.total_previsao_inicial, tp.total_previsao_atualizada, tp.total_receita_atual, tp.total_receita_anterior
        FROM dados_sumarizados ds
        JOIN totais_principais tp ON ds.{campo_principal} = tp.{campo_principal}
        WHERE (ABS(ds.previsao_inicial) + ABS(ds.previsao_atualizada) + ABS(ds.receita_atual) + ABS(ds.receita_anterior)) > 0.01
        ORDER BY tp.total_receita_atual DESC, ds.{campo_principal}, ds.receita_atual DESC
        """
        
        query_adaptada = adaptar_query(query_original)
        cursor = self.conn.cursor()
        cursor.execute(query_adaptada)
        
        resultados = []
        grupos = {}
        
        for row_dict in cursor:
            # --- CORREÇÃO APLICADA AQUI ---
            row = {str(k).lower(): v for k, v in dict(row_dict).items()}
            
            # --- CORREÇÃO: Usar nomes de campo em minúsculas
            codigo_principal = row[campo_principal.lower()]
            
            if codigo_principal not in grupos:
                grupos[codigo_principal] = {
                    'id': f'{tipo}-{codigo_principal}', 'codigo': codigo_principal, 'descricao': row['nome_principal'],
                    'tipo': 'principal', 'nivel': 0,
                    'previsao_inicial': row['total_previsao_inicial'] or 0, 'previsao_atualizada': row['total_previsao_atualizada'] or 0,
                    'receita_atual': row['total_receita_atual'] or 0, 'receita_anterior': row['total_receita_anterior'] or 0,
                    'tem_filhos': True, 'expandido': False, 'itens_secundarios': []
                }
            
            if row[campo_secundario.lower()]:
                item_secundario = {
                    'id': f'{tipo}-{codigo_principal}-{row[campo_secundario.lower()]}', 'codigo': row[campo_secundario.lower()],
                    'descricao': row['nome_secundario'], 'tipo': 'secundario', 'nivel': 1, 'pai_id': f'{tipo}-{codigo_principal}',
                    'previsao_inicial': row['previsao_inicial'] or 0, 'previsao_atualizada': row['previsao_atualizada'] or 0,
                    'receita_atual': row['receita_atual'] or 0, 'receita_anterior': row['receita_anterior'] or 0,
                    'tem_filhos': False,
                    'tem_lancamentos': self.estrutura['tem_lancamentos'] and coug and tipo == 'fonte' and (row['receita_atual'] != 0 or row['receita_anterior'] != 0),
                    'params_lancamentos': {'coalinea': row[campo_secundario.lower()] if tipo == 'fonte' else None, 'cofonte': codigo_principal if tipo == 'fonte' else None} if tipo == 'fonte' else None
                }
                grupos[codigo_principal]['itens_secundarios'].append(item_secundario)
        
        for grupo in grupos.values():
            self._calcular_variacoes(grupo)
            resultados.append(grupo)
            for item in grupo['itens_secundarios']:
                self._calcular_variacoes(item)
                resultados.append(item)
        
        return resultados

    # O restante da classe RelatorioReceitaFonte pode ser mantido.
    # ... (métodos _calcular_variacoes e calcular_totais)
    def _calcular_variacoes(self, item: Dict) -> None:
        """Calcula variações absolutas e percentuais"""
        item['variacao_absoluta'] = item['receita_atual'] - item['receita_anterior']
        
        if item['receita_anterior'] != 0:
            item['variacao_percentual'] = (
                item['variacao_absoluta'] / abs(item['receita_anterior']) # Usar abs para evitar divisão por negativo
            ) * 100
        else:
            item['variacao_percentual'] = 0 if item['variacao_absoluta'] == 0 else 100.0
    
    def calcular_totais(self, dados: List[Dict]) -> Dict:
        """Calcula totais gerais do relatório"""
        totais = {k: 0 for k in ['previsao_inicial', 'previsao_atualizada', 'receita_atual', 'receita_anterior', 'variacao_absoluta', 'variacao_percentual']}
        
        for item in dados:
            if item.get('nivel') == 0:
                totais['previsao_inicial'] += item['previsao_inicial']
                totais['previsao_atualizada'] += item['previsao_atualizada']
                totais['receita_atual'] += item['receita_atual']
                totais['receita_anterior'] += item['receita_anterior']
        
        totais['variacao_absoluta'] = totais['receita_atual'] - totais['receita_anterior']
        
        if totais['receita_anterior'] != 0:
            totais['variacao_percentual'] = (totais['variacao_absoluta'] / abs(totais['receita_anterior'])) * 100
        
        return totais

# Função auxiliar
def gerar_relatorio_receita_fonte(conn, tipo, ano, mes, coug=None, filtro_relatorio_key=None):
    relatorio = RelatorioReceitaFonte(conn)
    
    # Chama o método interno corrigido
    dados = relatorio._gerar_relatorio(
        tipo=tipo,
        ano=ano,
        mes=mes,
        coug=coug,
        filtro_relatorio_key=filtro_relatorio_key
    )
    
    totais = relatorio.calcular_totais(dados)
    
    return {
        'tipo': tipo,
        'dados': dados,
        'totais': totais,
        'tem_dados': len(dados) > 0,
        'coug_selecionada': coug
    }