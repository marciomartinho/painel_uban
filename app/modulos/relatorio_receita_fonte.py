# app/modulos/relatorio_receita_fonte.py
"""
Módulo para gerar relatórios agrupados por Código de Receita ou Código de Fonte
Permite visualização hierárquica com expansão/colapso
"""

import sqlite3
from typing import List, Dict, Optional, Literal
from app.modulos.formatacao import formatar_moeda
from app.modulos.regras_contabeis_receita import get_filtro_conta, FILTROS_RELATORIO_ESPECIAIS


class RelatorioReceitaFonte:
    """Classe para gerar relatórios agrupados por receita ou fonte"""
    
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.estrutura = self._verificar_estrutura()
        
    def _verificar_estrutura(self):
        """Verifica se o banco de lançamentos está disponível"""
        tem_lancamentos = False
        try:
            cursor = self.conn.execute("SELECT 1 FROM lancamentos_db.lancamentos LIMIT 1")
            tem_lancamentos = True
        except:
            pass
        return {'tem_lancamentos': tem_lancamentos}
        
    def gerar_relatorio_por_receita(self, ano: int, mes: int, 
                                   coug: Optional[str] = None, 
                                   filtro_relatorio_key: Optional[str] = None) -> List[Dict]:
        """
        Gera relatório agrupado por código de receita (COALINEA)
        Com opção de expandir por código de fonte (COFONTE)
        """
        return self._gerar_relatorio(
            tipo='receita',
            ano=ano,
            mes=mes,
            coug=coug,
            filtro_relatorio_key=filtro_relatorio_key
        )
    
    def gerar_relatorio_por_fonte(self, ano: int, mes: int, 
                                 coug: Optional[str] = None, 
                                 filtro_relatorio_key: Optional[str] = None) -> List[Dict]:
        """
        Gera relatório agrupado por código de fonte (COFONTE)
        Com opção de expandir por código de receita (COALINEA)
        """
        return self._gerar_relatorio(
            tipo='fonte',
            ano=ano,
            mes=mes,
            coug=coug,
            filtro_relatorio_key=filtro_relatorio_key
        )
    
    def _gerar_relatorio(self, tipo: Literal['receita', 'fonte'], 
                        ano: int, mes: int, 
                        coug: Optional[str] = None,
                        filtro_relatorio_key: Optional[str] = None) -> List[Dict]:
        """
        Gera o relatório baseado no tipo solicitado
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
        where_clause = " AND " + " AND ".join(filtros) if filtros else ""
        
        # Define campos de agrupamento baseado no tipo
        if tipo == 'receita':
            campo_principal = 'COALINEA'
            nome_principal = 'NOALINEA'
            tabela_principal = 'alineas'
            campo_secundario = 'COFONTE'
            nome_secundario = 'NOFONTE'
            tabela_secundaria = 'fontes'
        else:  # tipo == 'fonte'
            campo_principal = 'COFONTE'
            nome_principal = 'NOFONTE'
            tabela_principal = 'fontes'
            campo_secundario = 'COALINEA'
            nome_secundario = 'NOALINEA'
            tabela_secundaria = 'alineas'
        
        # Query principal
        query = f"""
        WITH dados_agregados AS (
            SELECT 
                fs.{campo_principal},
                COALESCE(dp.{nome_principal}, 'Código ' || fs.{campo_principal}) as nome_principal,
                fs.{campo_secundario},
                COALESCE(ds.{nome_secundario}, 'Código ' || fs.{campo_secundario}) as nome_secundario,
                fs.COEXERCICIO,
                fs.INMES,
                
                -- Previsão Inicial
                SUM(CASE 
                    WHEN {get_filtro_conta('PREVISAO_INICIAL_LIQUIDA')} 
                    THEN fs.saldo_contabil 
                    ELSE 0 
                END) as previsao_inicial,
                
                -- Previsão Atualizada
                SUM(CASE 
                    WHEN {get_filtro_conta('PREVISAO_ATUALIZADA_LIQUIDA')} 
                    THEN fs.saldo_contabil 
                    ELSE 0 
                END) as previsao_atualizada,
                
                -- Receita Realizada
                SUM(CASE 
                    WHEN {get_filtro_conta('RECEITA_LIQUIDA')} 
                    THEN fs.saldo_contabil 
                    ELSE 0 
                END) as receita_liquida
                
            FROM fato_saldos fs
            LEFT JOIN dimensoes.{tabela_principal} dp 
                ON fs.{campo_principal} = dp.CO{campo_principal.replace('CO', '')}
            LEFT JOIN dimensoes.{tabela_secundaria} ds 
                ON fs.{campo_secundario} = ds.CO{campo_secundario.replace('CO', '')}
            WHERE fs.{campo_principal} IS NOT NULL 
                AND fs.{campo_principal} != ''
                {where_clause}
            GROUP BY fs.{campo_principal}, dp.{nome_principal}, 
                     fs.{campo_secundario}, ds.{nome_secundario},
                     fs.COEXERCICIO, fs.INMES
        ),
        dados_sumarizados AS (
            SELECT 
                {campo_principal},
                nome_principal,
                {campo_secundario},
                nome_secundario,
                
                -- Valores do ano atual
                SUM(CASE 
                    WHEN COEXERCICIO = {ano} 
                    THEN previsao_inicial 
                    ELSE 0 
                END) as previsao_inicial,
                
                SUM(CASE 
                    WHEN COEXERCICIO = {ano} 
                    THEN previsao_atualizada 
                    ELSE 0 
                END) as previsao_atualizada,
                
                SUM(CASE 
                    WHEN COEXERCICIO = {ano} 
                    AND INMES <= {mes}
                    THEN receita_liquida 
                    ELSE 0 
                END) as receita_atual,
                
                -- Valores do ano anterior
                SUM(CASE 
                    WHEN COEXERCICIO = {ano-1} 
                    AND INMES <= {mes}
                    THEN receita_liquida 
                    ELSE 0 
                END) as receita_anterior
                
            FROM dados_agregados
            WHERE COEXERCICIO IN ({ano}, {ano-1})
            GROUP BY {campo_principal}, nome_principal, 
                     {campo_secundario}, nome_secundario
        ),
        -- Agrupa totais por item principal
        totais_principais AS (
            SELECT 
                {campo_principal},
                nome_principal,
                SUM(previsao_inicial) as total_previsao_inicial,
                SUM(previsao_atualizada) as total_previsao_atualizada,
                SUM(receita_atual) as total_receita_atual,
                SUM(receita_anterior) as total_receita_anterior
            FROM dados_sumarizados
            GROUP BY {campo_principal}, nome_principal
        )
        SELECT 
            ds.*,
            tp.total_previsao_inicial,
            tp.total_previsao_atualizada,
            tp.total_receita_atual,
            tp.total_receita_anterior
        FROM dados_sumarizados ds
        JOIN totais_principais tp 
            ON ds.{campo_principal} = tp.{campo_principal}
        WHERE (ABS(ds.previsao_inicial) + ABS(ds.previsao_atualizada) + 
               ABS(ds.receita_atual) + ABS(ds.receita_anterior)) > 0.01
        ORDER BY tp.total_receita_atual DESC, 
                 ds.{campo_principal}, 
                 ds.receita_atual DESC
        """
        
        cursor = self.conn.execute(query)
        
        # Processa resultados em estrutura hierárquica
        resultados = []
        grupos = {}
        
        for row in cursor:
            codigo_principal = row[campo_principal]
            
            # Se ainda não existe o grupo principal, cria
            if codigo_principal not in grupos:
                grupos[codigo_principal] = {
                    'id': f'{tipo}-{codigo_principal}',
                    'codigo': codigo_principal,
                    'descricao': row['nome_principal'],
                    'tipo': 'principal',
                    'nivel': 0,
                    'previsao_inicial': row['total_previsao_inicial'] or 0,
                    'previsao_atualizada': row['total_previsao_atualizada'] or 0,
                    'receita_atual': row['total_receita_atual'] or 0,
                    'receita_anterior': row['total_receita_anterior'] or 0,
                    'tem_filhos': True,
                    'expandido': False,
                    'itens_secundarios': []
                }
            
            # Adiciona item secundário
            if row[campo_secundario]:
                item_secundario = {
                    'id': f'{tipo}-{codigo_principal}-{row[campo_secundario]}',
                    'codigo': row[campo_secundario],
                    'descricao': row['nome_secundario'],
                    'tipo': 'secundario',
                    'nivel': 1,
                    'pai_id': f'{tipo}-{codigo_principal}',
                    'previsao_inicial': row['previsao_inicial'] or 0,
                    'previsao_atualizada': row['previsao_atualizada'] or 0,
                    'receita_atual': row['receita_atual'] or 0,
                    'receita_anterior': row['receita_anterior'] or 0,
                    'tem_filhos': False,
                    # Adiciona informações para o modal de lançamentos quando for relatório por fonte
                    'tem_lancamentos': self.estrutura['tem_lancamentos'] and coug and tipo == 'fonte' and (
                        row['receita_atual'] != 0 or row['receita_anterior'] != 0
                    ),
                    'params_lancamentos': {
                        'coalinea': row[campo_secundario] if tipo == 'fonte' else None,
                        'cofonte': codigo_principal if tipo == 'fonte' else None
                    } if tipo == 'fonte' else None
                }
                
                grupos[codigo_principal]['itens_secundarios'].append(item_secundario)
        
        # Converte para lista e calcula variações
        for grupo in grupos.values():
            # Calcula variação do grupo principal
            self._calcular_variacoes(grupo)
            resultados.append(grupo)
            
            # Adiciona itens secundários
            for item in grupo['itens_secundarios']:
                self._calcular_variacoes(item)
                resultados.append(item)
        
        return resultados
    
    def _calcular_variacoes(self, item: Dict) -> None:
        """Calcula variações absolutas e percentuais"""
        item['variacao_absoluta'] = item['receita_atual'] - item['receita_anterior']
        
        if item['receita_anterior'] != 0:
            item['variacao_percentual'] = (
                item['variacao_absoluta'] / item['receita_anterior']
            ) * 100
        else:
            item['variacao_percentual'] = 0
    
    def calcular_totais(self, dados: List[Dict]) -> Dict:
        """Calcula totais gerais do relatório"""
        totais = {
            'previsao_inicial': 0,
            'previsao_atualizada': 0,
            'receita_atual': 0,
            'receita_anterior': 0,
            'variacao_absoluta': 0,
            'variacao_percentual': 0
        }
        
        # Soma apenas itens principais (nivel 0) para evitar dupla contagem
        for item in dados:
            if item.get('nivel') == 0:
                totais['previsao_inicial'] += item['previsao_inicial']
                totais['previsao_atualizada'] += item['previsao_atualizada']
                totais['receita_atual'] += item['receita_atual']
                totais['receita_anterior'] += item['receita_anterior']
        
        totais['variacao_absoluta'] = totais['receita_atual'] - totais['receita_anterior']
        
        if totais['receita_anterior'] != 0:
            totais['variacao_percentual'] = (
                totais['variacao_absoluta'] / totais['receita_anterior']
            ) * 100
        
        return totais


# Funções auxiliares para facilitar o uso

def gerar_relatorio_receita_fonte(conn: sqlite3.Connection, 
                                 tipo: str,
                                 ano: int, 
                                 mes: int,
                                 coug: Optional[str] = None,
                                 filtro_relatorio_key: Optional[str] = None) -> Dict:
    """
    Função auxiliar para gerar relatório completo
    
    Args:
        conn: Conexão com banco de dados
        tipo: 'receita' ou 'fonte'
        ano: Ano de referência
        mes: Mês de referência
        coug: Código da unidade gestora (opcional)
        filtro_relatorio_key: Chave do filtro de receita (opcional)
    
    Returns:
        Dicionário com dados e totais
    """
    relatorio = RelatorioReceitaFonte(conn)
    
    if tipo == 'receita':
        dados = relatorio.gerar_relatorio_por_receita(ano, mes, coug, filtro_relatorio_key)
    else:
        dados = relatorio.gerar_relatorio_por_fonte(ano, mes, coug, filtro_relatorio_key)
    
    totais = relatorio.calcular_totais(dados)
    
    return {
        'tipo': tipo,
        'dados': dados,
        'totais': totais,
        'tem_dados': len(dados) > 0,
        'coug_selecionada': coug
    }