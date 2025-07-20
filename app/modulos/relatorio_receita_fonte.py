# app/modulos/relatorio_receita_fonte.py
"""
Módulo para gerar relatórios agrupados por Código de Receita ou Código de Fonte
Permite visualização hierárquica com expansão/colapso
"""
import sqlite3
from typing import List, Dict, Optional, Literal
import psycopg2.extras
from .conexao_hibrida import adaptar_query, get_db_environment
from app.modulos.formatacao import formatar_moeda
from app.modulos.regras_contabeis_receita import get_filtro_conta, FILTROS_RELATORIO_ESPECIAIS


class RelatorioReceitaFonte:
    """Classe para gerar relatórios agrupados por receita ou fonte"""

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.estrutura = self._verificar_estrutura()

    def _verificar_estrutura(self):
        """Verifica estrutura disponível no banco"""
        tem_lancamentos = False
        tem_tabela_fontes = False
        
        try:
            cursor = self.conn.cursor()
            
            # Verifica se tem lançamentos - tentativa múltipla para SQLite
            if get_db_environment() == 'postgres':
                query = "SELECT 1 FROM information_schema.tables WHERE table_name = 'lancamentos' LIMIT 1"
                cursor.execute(query)
                if cursor.fetchone():
                    tem_lancamentos = True
            else:
                # Para SQLite, tenta várias abordagens
                queries_tentativa = [
                    "SELECT 1 FROM lancamentos LIMIT 1",
                    "SELECT 1 FROM lancamentos_db.lancamentos LIMIT 1",
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='lancamentos' LIMIT 1"
                ]
                
                for query in queries_tentativa:
                    try:
                        cursor.execute(query)
                        if cursor.fetchone() is not None:
                            tem_lancamentos = True
                            print(f"DEBUG - Tabela lancamentos encontrada com query: {query}")
                            break
                    except Exception as e:
                        continue
            
            # Verifica se tem tabela fontes
            if get_db_environment() == 'postgres':
                query = "SELECT 1 FROM information_schema.tables WHERE table_schema = 'dimensoes' AND table_name = 'fontes' LIMIT 1"
                cursor.execute(query)
                if cursor.fetchone():
                    tem_tabela_fontes = True
            else:
                try:
                    cursor.execute("SELECT 1 FROM dimensoes.fontes LIMIT 1")
                    tem_tabela_fontes = True
                except:
                    try:
                        cursor.execute("SELECT 1 FROM fontes LIMIT 1")
                        tem_tabela_fontes = True
                    except:
                        tem_tabela_fontes = False
                        
        except Exception as e:
            print(f"Erro ao verificar estrutura: {e}")
            
        resultado = {
            'tem_lancamentos': tem_lancamentos,
            'tem_tabela_fontes': tem_tabela_fontes
        }
        print(f"DEBUG - Estrutura verificada: {resultado}")
        return resultado

    def _gerar_relatorio(self, tipo: Literal['receita', 'fonte'],
                        ano: int, mes: int,
                        coug: Optional[str] = None,
                        filtro_relatorio_key: Optional[str] = None) -> List[Dict]:

        print(f"DEBUG - _gerar_relatorio chamado com: tipo={tipo}, ano={ano}, mes={mes}, coug={coug}, filtro={filtro_relatorio_key}")

        filtros = []
        if coug:
            coug_escaped = coug.replace("'", "''")
            filtros.append(f"fs.coug = '{coug_escaped}'")

        if filtro_relatorio_key and filtro_relatorio_key in FILTROS_RELATORIO_ESPECIAIS:
            regra = FILTROS_RELATORIO_ESPECIAIS[filtro_relatorio_key]
            campo = regra['campo_filtro']
            valores_str = ", ".join([f"'{v}'" for v in regra['valores']])
            filtros.append(f"fs.{campo} IN ({valores_str})")

        where_clause = " AND " + " AND ".join(filtros) if filtros else ""

        # Define configuração baseada no tipo
        if tipo == 'receita':
            campo_principal = 'coalinea'
            nome_principal = 'noalinea'
            tabela_principal = 'alineas'
            campo_secundario = 'cofonte'
            nome_secundario = 'nofonte' if self.estrutura['tem_tabela_fontes'] else None
            tabela_secundaria = 'fontes' if self.estrutura['tem_tabela_fontes'] else None
        else:
            campo_principal = 'cofonte'
            nome_principal = 'nofonte' if self.estrutura['tem_tabela_fontes'] else None
            tabela_principal = 'fontes' if self.estrutura['tem_tabela_fontes'] else None
            campo_secundario = 'coalinea'
            nome_secundario = 'noalinea'
            tabela_secundaria = 'alineas'
        
        # Define type casts para PostgreSQL
        if get_db_environment() == 'postgres':
            type_cast_text = "::text"
            type_cast_int = "::integer"
        else:
            type_cast_text = ""
            type_cast_int = ""

        # Monta joins condicionais
        join_principal = ""
        campo_nome_principal = f"'Código ' || fs.{campo_principal}"
        
        if tabela_principal and nome_principal:
            if get_db_environment() == 'postgres':
                join_principal = f"LEFT JOIN dimensoes.{tabela_principal} dp ON fs.{campo_principal}{type_cast_text} = dp.{campo_principal}"
            else:
                join_principal = f"LEFT JOIN dimensoes.{tabela_principal} dp ON fs.{campo_principal} = dp.{campo_principal}"
            campo_nome_principal = f"COALESCE(dp.{nome_principal}, 'Código ' || fs.{campo_principal})"
        
        join_secundario = ""
        campo_nome_secundario = f"'Código ' || fs.{campo_secundario}"
        
        if tabela_secundaria and nome_secundario:
            if get_db_environment() == 'postgres':
                join_secundario = f"LEFT JOIN dimensoes.{tabela_secundaria} ds ON fs.{campo_secundario}{type_cast_text} = ds.{campo_secundario}"
            else:
                join_secundario = f"LEFT JOIN dimensoes.{tabela_secundaria} ds ON fs.{campo_secundario} = ds.{campo_secundario}"
            campo_nome_secundario = f"COALESCE(ds.{nome_secundario}, 'Código ' || fs.{campo_secundario})"

        # Query principal com todos os ajustes
        query_original = f"""
        WITH dados_agregados AS (
            SELECT
                fs.{campo_principal},
                {campo_nome_principal} as nome_principal,
                fs.{campo_secundario},
                {campo_nome_secundario} as nome_secundario,
                fs.coexercicio,
                fs.inmes,
                SUM(CASE WHEN {get_filtro_conta('PREVISAO_INICIAL_LIQUIDA')} THEN COALESCE(fs.saldo_contabil, 0) ELSE 0 END) as previsao_inicial,
                SUM(CASE WHEN {get_filtro_conta('PREVISAO_ATUALIZADA_LIQUIDA')} THEN COALESCE(fs.saldo_contabil, 0) ELSE 0 END) as previsao_atualizada,
                SUM(CASE WHEN {get_filtro_conta('RECEITA_LIQUIDA')} THEN COALESCE(fs.saldo_contabil, 0) ELSE 0 END) as receita_liquida
            FROM fato_saldos fs
            {join_principal}
            {join_secundario}
            WHERE fs.{campo_principal} IS NOT NULL {where_clause}
            GROUP BY 1, 2, 3, 4, 5, 6
        ),
        dados_sumarizados AS (
            SELECT
                {campo_principal}, 
                nome_principal, 
                {campo_secundario}, 
                nome_secundario,
                SUM(CASE WHEN coexercicio{type_cast_int} = {ano} THEN previsao_inicial ELSE 0 END) as previsao_inicial,
                SUM(CASE WHEN coexercicio{type_cast_int} = {ano} THEN previsao_atualizada ELSE 0 END) as previsao_atualizada,
                SUM(CASE WHEN coexercicio{type_cast_int} = {ano} AND inmes{type_cast_int} <= {mes} THEN receita_liquida ELSE 0 END) as receita_atual,
                SUM(CASE WHEN coexercicio{type_cast_int} = {ano-1} AND inmes{type_cast_int} <= {mes} THEN receita_liquida ELSE 0 END) as receita_anterior
            FROM dados_agregados 
            WHERE coexercicio{type_cast_int} IN ({ano}, {ano-1})
            GROUP BY 1, 2, 3, 4
        ),
        totais_principais AS (
            SELECT
                {campo_principal}, 
                nome_principal,
                SUM(previsao_inicial) as total_previsao_inicial, 
                SUM(previsao_atualizada) as total_previsao_atualizada,
                SUM(receita_atual) as total_receita_atual, 
                SUM(receita_anterior) as total_receita_anterior
            FROM dados_sumarizados 
            GROUP BY 1, 2
        )
        SELECT 
            ds.*, 
            tp.total_previsao_inicial, 
            tp.total_previsao_atualizada, 
            tp.total_receita_atual, 
            tp.total_receita_anterior
        FROM dados_sumarizados ds
        JOIN totais_principais tp ON ds.{campo_principal} = tp.{campo_principal}
        WHERE (ABS(COALESCE(ds.previsao_inicial, 0)) + ABS(COALESCE(ds.previsao_atualizada, 0)) + 
               ABS(COALESCE(ds.receita_atual, 0)) + ABS(COALESCE(ds.receita_anterior, 0))) > 0.01
        ORDER BY tp.total_receita_atual DESC, ds.{campo_principal}, ds.receita_atual DESC
        """

        query_adaptada = adaptar_query(query_original)
        
        try:
            if get_db_environment() == 'postgres':
                cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            else:
                cursor = self.conn.cursor()
                
            cursor.execute(query_adaptada)

            resultados = []
            grupos = {}

            for row in cursor:
                # Converte para dict de forma segura
                if get_db_environment() == 'postgres':
                    row_dict = dict(row)
                else:
                    row_dict = dict(zip([d[0] for d in cursor.description], row))
                
                codigo_principal = str(row_dict.get(campo_principal, ''))
                if not codigo_principal:
                    continue

                if codigo_principal not in grupos:
                    grupos[codigo_principal] = {
                        'id': f'{tipo}-{codigo_principal}',
                        'codigo': codigo_principal,
                        'descricao': row_dict.get('nome_principal', f'Código {codigo_principal}'),
                        'tipo': 'principal',
                        'nivel': 0,
                        'previsao_inicial': float(row_dict.get('total_previsao_inicial', 0) or 0),
                        'previsao_atualizada': float(row_dict.get('total_previsao_atualizada', 0) or 0),
                        'receita_atual': float(row_dict.get('total_receita_atual', 0) or 0),
                        'receita_anterior': float(row_dict.get('total_receita_anterior', 0) or 0),
                        'tem_filhos': True,
                        'expandido': False,
                        'itens_secundarios': []
                    }

                codigo_secundario = row_dict.get(campo_secundario)
                if codigo_secundario:
                    receita_atual = float(row_dict.get('receita_atual', 0) or 0)
                    receita_anterior = float(row_dict.get('receita_anterior', 0) or 0)
                    
                    # Determina se deve mostrar botão de lançamentos
                    deve_mostrar_lancamentos = (
                        bool(coug) and  # Tem UG selecionada
                        tipo == 'fonte' and  # É relatório por fonte
                        (receita_atual != 0 or receita_anterior != 0)  # Tem movimento
                    )
                    
                    print(f"DEBUG - Item secundário: codigo={codigo_secundario}, coug={coug}, tipo={tipo}, receita_atual={receita_atual}, deve_mostrar_lancamentos={deve_mostrar_lancamentos}")
                    
                    item_secundario = {
                        'id': f'{tipo}-{codigo_principal}-{codigo_secundario}',
                        'codigo': str(codigo_secundario),
                        'descricao': row_dict.get('nome_secundario', f'Código {codigo_secundario}'),
                        'tipo': 'secundario',
                        'nivel': 1,
                        'pai_id': f'{tipo}-{codigo_principal}',
                        'previsao_inicial': float(row_dict.get('previsao_inicial', 0) or 0),
                        'previsao_atualizada': float(row_dict.get('previsao_atualizada', 0) or 0),
                        'receita_atual': receita_atual,
                        'receita_anterior': receita_anterior,
                        'tem_filhos': False,
                        'tem_lancamentos': deve_mostrar_lancamentos,
                        'params_lancamentos': {
                            'coalinea': str(codigo_secundario) if tipo == 'fonte' else None,
                            'cofonte': str(codigo_principal) if tipo == 'fonte' else None
                        } if tipo == 'fonte' and deve_mostrar_lancamentos else None
                    }
                    grupos[codigo_principal]['itens_secundarios'].append(item_secundario)

            # Processa os resultados
            for grupo in grupos.values():
                self._calcular_variacoes(grupo)
                resultados.append(grupo)
                for item in grupo['itens_secundarios']:
                    self._calcular_variacoes(item)
                    resultados.append(item)

            print(f"DEBUG - Total de resultados processados: {len(resultados)}")
            return resultados
            
        except Exception as e:
            print(f"Erro ao gerar relatório: {e}")
            import traceback
            traceback.print_exc()
            return []

    def _calcular_variacoes(self, item: Dict) -> None:
        """Calcula variações absolutas e percentuais"""
        receita_atual = item.get('receita_atual', 0) or 0
        receita_anterior = item.get('receita_anterior', 0) or 0
        
        item['variacao_absoluta'] = receita_atual - receita_anterior
        
        if receita_anterior != 0:
            item['variacao_percentual'] = (item['variacao_absoluta'] / abs(receita_anterior)) * 100
        else:
            item['variacao_percentual'] = 100.0 if item['variacao_absoluta'] != 0 else 0.0

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
        
        for item in dados:
            if item.get('nivel') == 0:  # Apenas itens principais
                totais['previsao_inicial'] += item.get('previsao_inicial', 0)
                totais['previsao_atualizada'] += item.get('previsao_atualizada', 0)
                totais['receita_atual'] += item.get('receita_atual', 0)
                totais['receita_anterior'] += item.get('receita_anterior', 0)
                
        totais['variacao_absoluta'] = totais['receita_atual'] - totais['receita_anterior']
        
        if totais['receita_anterior'] != 0:
            totais['variacao_percentual'] = (totais['variacao_absoluta'] / abs(totais['receita_anterior'])) * 100
        else:
            totais['variacao_percentual'] = 100.0 if totais['variacao_absoluta'] != 0 else 0.0
            
        return totais


def gerar_relatorio_receita_fonte(conn, tipo, ano, mes, coug=None, filtro_relatorio_key=None):
    """Função auxiliar para gerar o relatório"""
    try:
        print(f"DEBUG - gerar_relatorio_receita_fonte: tipo={tipo}, ano={ano}, mes={mes}, coug={coug}")
        
        relatorio = RelatorioReceitaFonte(conn)
        dados = relatorio._gerar_relatorio(
            tipo=tipo, 
            ano=ano, 
            mes=mes, 
            coug=coug, 
            filtro_relatorio_key=filtro_relatorio_key
        )
        totais = relatorio.calcular_totais(dados)
        
        resultado = {
            'tipo': tipo,
            'dados': dados,
            'totais': totais,
            'tem_dados': len(dados) > 0,
            'coug_selecionada': coug,
            'estrutura': relatorio.estrutura
        }
        
        print(f"DEBUG - Resultado final: tem_dados={resultado['tem_dados']}, coug_selecionada={resultado['coug_selecionada']}")
        
        return resultado
        
    except Exception as e:
        print(f"Erro ao gerar relatório receita/fonte: {e}")
        import traceback
        traceback.print_exc()
        
        return {
            'tipo': tipo,
            'dados': [],
            'totais': {},
            'tem_dados': False,
            'coug_selecionada': coug,
            'erro': str(e)
        }