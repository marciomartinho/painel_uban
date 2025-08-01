# app/routes_relatorios.py
"""
Rotas para os relatórios do sistema orçamentário.
Versão refatorada para usar o módulo de conexão híbrida.
"""

from flask import Blueprint, render_template, request, send_file, jsonify
import pandas as pd
from io import BytesIO
from datetime import datetime
import traceback
import psycopg2
import psycopg2.extras
import sqlite3

from app.modulos.conexao_hibrida import ConexaoBanco, adaptar_query, get_db_environment
from app.modulos.periodo import obter_periodo_referencia
from app.modulos.formatacao import formatar_moeda, formatar_percentual
from app.modulos.regras_contabeis_receita import get_filtro_conta, FILTROS_RELATORIO_ESPECIAIS
from app.modulos.coug_manager import COUGManager
from app.modulos.comparativo_mensal import gerar_comparativo_mensal
from app.modulos.cards_unidades_gestoras import gerar_cards_unidades
from app.modulos.relatorio_receita_fonte import gerar_relatorio_receita_fonte
from app.modulos.modal_lancamentos import processar_requisicao_lancamentos, gerar_botao_lancamentos

relatorios_bp = Blueprint('relatorios', __name__, url_prefix='/relatorios')


def gerar_resumo_executivo(dados):
    if not dados or len(dados) <= 1: return None
    try:
        total_geral = next((item for item in dados if item.get('id') == 'total'), None)
        if not total_geral: return None
        resumo = {
            'total_geral': {
                'receita_2025': total_geral.get('receita_atual', 0),
                'receita_2024': total_geral.get('receita_anterior', 0),
                'variacao_abs': total_geral.get('variacao_absoluta', 0),
                'variacao_pct': total_geral.get('variacao_percentual', 0)
            }
        }
        categorias = [d for d in dados if d.get('nivel') == 0]
        if categorias:
            cat_principal = max(categorias, key=lambda x: x.get('receita_atual', 0))
            resumo['categoria_principal'] = {'descricao': cat_principal['descricao'], 'valor': cat_principal['receita_atual']}
        return resumo
    except Exception as e:
        print(f"Erro ao gerar resumo executivo: {e}")
        return None

def exportar_excel_balanco(dados, periodo, coug_selecionada, coug_manager, filtro_relatorio_key=None):
    rows = []
    for item in dados:
        if item.get('nivel', -2) >= -1:
            indent = '    ' * max(0, item.get('nivel', 0))
            row = {
                'Código': item.get('codigo', ''), 'Descrição': indent + item.get('descricao', ''),
                f'Previsão Inicial {periodo["ano"]}': item.get('previsao_inicial', 0),
                f'Previsão Atualizada {periodo["ano"]}': item.get('previsao_atualizada', 0),
                f'Receita Realizada {periodo["mes"]}/{periodo["ano"]}': item.get('receita_atual', 0),
                f'Receita Realizada {periodo["mes"]}/{periodo["ano"]-1}': item.get('receita_anterior', 0),
                'Variação Absoluta': item.get('variacao_absoluta', 0),
                'Variação %': item.get('variacao_percentual', 0) / 100
            }
            rows.append(row)
    df = pd.DataFrame(rows)
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='Balanço Orçamentário', index=False)
        worksheet = writer.sheets['Balanço Orçamentário']
        worksheet.column_dimensions['B'].width = 60
        for col_letter in ['C', 'D', 'E', 'F', 'G']:
            worksheet.column_dimensions[col_letter].width = 22
            for cell in worksheet[col_letter][1:]: cell.number_format = 'R$ #,##0.00'
        worksheet.column_dimensions['H'].width = 15
        for cell in worksheet['H'][1:]: cell.number_format = '0.00%'
    output.seek(0)
    sufixo_coug = coug_manager.get_sufixo_arquivo(coug_selecionada)
    sufixo_filtro = f"_{filtro_relatorio_key}" if filtro_relatorio_key else ""
    filename = f'balanco_orcamentario_receita{sufixo_coug}{sufixo_filtro}_{periodo["ano"]}_{periodo["mes"]:02d}.xlsx'
    return send_file(output, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', as_attachment=True, download_name=filename)


class ProcessadorDadosReceita:
    """Processa dados para o relatório de balanço orçamentário"""
    def __init__(self, conn):
        self.conn = conn
        if get_db_environment() == 'postgres':
            self.cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        else:
            self.conn.row_factory = sqlite3.Row
            self.cursor = conn.cursor()
        self.coug_manager = COUGManager(self.conn)

    def buscar_dados_balanco(self, mes, ano, coug=None, filtro_relatorio_key=None):
        query_original = self._montar_query_agregada(mes, ano, coug, filtro_relatorio_key)
        query_adaptada = adaptar_query(query_original)
        try:
            self.cursor.execute(query_adaptada)
            resultados = self.cursor.fetchall()
            return self._processar_resultados_agregados(resultados)
        except Exception as e:
            print(f"Erro ao buscar dados agregados: {e}")
            traceback.print_exc()
            return []

    def _montar_query_agregada(self, mes, ano, coug=None, filtro_relatorio_key=None):
        filtro_coug = self.coug_manager.aplicar_filtro_query("fs", coug)
        filtro_dinamico = ""
        if filtro_relatorio_key and filtro_relatorio_key in FILTROS_RELATORIO_ESPECIAIS:
            regra = FILTROS_RELATORIO_ESPECIAIS[filtro_relatorio_key]
            campo = regra['campo_filtro']
            valores_str = ", ".join([f"'{v}'" for v in regra['valores']])
            filtro_dinamico = f"AND fs.{campo} IN ({valores_str})"
        
        # --- CORREÇÃO APLICADA AQUI ---
        type_cast_text = "::text" if get_db_environment() == 'postgres' else ""
        type_cast_numeric = "::integer" if get_db_environment() == 'postgres' else ""

        return f"""
        WITH dados_agregados AS (
            SELECT
                fs.categoriareceita,
                COALESCE(cat.nocategoriareceita, 'Categoria ' || fs.categoriareceita) as nome_categoria,
                fs.cofontereceita,
                COALESCE(ori.nofontereceita, 'Fonte ' || fs.cofontereceita) as nome_fonte,
                fs.cosubfontereceita,
                COALESCE(esp.nosubfontereceita, 'Subfonte ' || fs.cosubfontereceita) as nome_subfonte,
                fs.coalinea,
                COALESCE(ali.noalinea, 'Alínea ' || fs.coalinea) as nome_alinea,
                fs.coexercicio,
                fs.inmes,
                SUM(CASE WHEN {get_filtro_conta('PREVISAO_INICIAL_LIQUIDA')} THEN COALESCE(fs.saldo_contabil, 0) ELSE 0 END) as previsao_inicial,
                SUM(CASE WHEN {get_filtro_conta('PREVISAO_ATUALIZADA_LIQUIDA')} THEN COALESCE(fs.saldo_contabil, 0) ELSE 0 END) as previsao_atualizada,
                SUM(CASE WHEN {get_filtro_conta('RECEITA_LIQUIDA')} THEN COALESCE(fs.saldo_contabil, 0) ELSE 0 END) as receita_liquida
            FROM fato_saldos fs
            LEFT JOIN dimensoes.categorias cat ON fs.categoriareceita{type_cast_text} = cat.cocategoriareceita
            LEFT JOIN dimensoes.origens ori ON fs.cofontereceita{type_cast_text} = ori.cofontereceita
            LEFT JOIN dimensoes.especies esp ON fs.cosubfontereceita{type_cast_text} = esp.cosubfontereceita
            LEFT JOIN dimensoes.alineas ali ON fs.coalinea{type_cast_text} = ali.coalinea
            WHERE 1=1 {filtro_coug} {filtro_dinamico}
            GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
        ),
        dados_calculados AS (
            SELECT
                categoriareceita, nome_categoria, cofontereceita, nome_fonte,
                cosubfontereceita, nome_subfonte, coalinea, nome_alinea,
                SUM(CASE WHEN coexercicio{type_cast_numeric} = {ano} THEN previsao_inicial ELSE 0 END) as previsao_inicial,
                SUM(CASE WHEN coexercicio{type_cast_numeric} = {ano} THEN previsao_atualizada ELSE 0 END) as previsao_atualizada,
                SUM(CASE WHEN coexercicio{type_cast_numeric} = {ano} AND inmes{type_cast_numeric} <= {mes} THEN receita_liquida ELSE 0 END) as receita_atual,
                SUM(CASE WHEN coexercicio{type_cast_numeric} = {ano-1} AND inmes{type_cast_numeric} <= {mes} THEN receita_liquida ELSE 0 END) as receita_anterior
            FROM dados_agregados
            WHERE coexercicio{type_cast_numeric} IN ({ano}, {ano-1})
            GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
        )
        SELECT * FROM dados_calculados
        WHERE (ABS(previsao_inicial) + ABS(previsao_atualizada) + ABS(receita_atual) + ABS(receita_anterior)) > 0.01
        ORDER BY categoriareceita, cofontereceita, cosubfontereceita, coalinea
        """

    def _processar_resultados_agregados(self, resultados):
        if not resultados:
            return []
        hierarquia = {}
        for row_dict in resultados:
            self._adicionar_na_hierarquia(hierarquia, dict(row_dict))
        dados_processados = []
        self._hierarquia_para_lista(hierarquia, dados_processados)
        total_geral = self._calcular_total_geral(dados_processados)
        dados_processados.append(total_geral)
        return dados_processados

    def _adicionar_na_hierarquia(self, hierarquia, row):
        cat_id = str(row.get('categoriareceita'))
        fonte_id = str(row.get('cofontereceita'))
        subfonte_id = str(row.get('cosubfontereceita'))
        alinea_id = str(row.get('coalinea'))
        if not cat_id: return
        if cat_id not in hierarquia: hierarquia[cat_id] = {'id': f'cat-{cat_id}', 'codigo': cat_id, 'descricao': row.get('nome_categoria'), 'nivel': 0, 'classes': 'nivel-0 parent-row', 'fontes': {}, **self._valores_zerados()}
        categoria = hierarquia[cat_id]
        if fonte_id and fonte_id not in categoria['fontes']: categoria['fontes'][fonte_id] = {'id': f'fonte-{cat_id}-{fonte_id}', 'codigo': fonte_id, 'descricao': row.get('nome_fonte'), 'nivel': 1, 'classes': 'nivel-1 parent-row', 'subfontes': {}, **self._valores_zerados()}
        fonte = categoria['fontes'].get(fonte_id)
        if not fonte: return
        if subfonte_id and subfonte_id not in fonte['subfontes']: fonte['subfontes'][subfonte_id] = {'id': f'sub-{cat_id}-{fonte_id}-{subfonte_id}', 'codigo': subfonte_id, 'descricao': row.get('nome_subfonte'), 'nivel': 2, 'classes': 'nivel-2 parent-row', 'alineas': {}, **self._valores_zerados()}
        subfonte = fonte['subfontes'].get(subfonte_id)
        if not subfonte: return
        if alinea_id:
            subfonte['alineas'][alinea_id] = {'id': f'ali-{cat_id}-{fonte_id}-{subfonte_id}-{alinea_id}', 'codigo': alinea_id, 'descricao': f"{row.get('coalinea')} - {row.get('nome_alinea')}", 'nivel': 3, 'classes': 'nivel-3', 'previsao_inicial': row.get('previsao_inicial', 0) or 0, 'previsao_atualizada': row.get('previsao_atualizada', 0) or 0, 'receita_atual': row.get('receita_atual', 0) or 0, 'receita_anterior': row.get('receita_anterior', 0) or 0, 'tem_lancamentos': (row.get('receita_atual', 0) != 0 or row.get('receita_anterior', 0) != 0), 'params_lancamentos': {'cat_id': cat_id, 'fonte_id': fonte_id, 'subfonte_id': subfonte_id, 'alinea_id': alinea_id}}
            for campo in ['previsao_inicial', 'previsao_atualizada', 'receita_atual', 'receita_anterior']:
                valor = row.get(campo, 0) or 0
                subfonte[campo] += valor
                fonte[campo] += valor
                categoria[campo] += valor

    def _hierarquia_para_lista(self, hierarquia, lista_saida):
        for cat_id in sorted(hierarquia.keys()):
            categoria = hierarquia[cat_id]
            self._calcular_variacoes(categoria)
            lista_saida.append({k: v for k, v in categoria.items() if k != 'fontes'})
            for fonte_id in sorted(categoria['fontes'].keys()):
                fonte = categoria['fontes'][fonte_id]
                self._calcular_variacoes(fonte)
                lista_saida.append({k: v for k, v in fonte.items() if k != 'subfontes'})
                for subfonte_id in sorted(fonte['subfontes'].keys()):
                    subfonte = fonte['subfontes'][subfonte_id]
                    self._calcular_variacoes(subfonte)
                    lista_saida.append({k: v for k, v in subfonte.items() if k != 'alineas'})
                    for alinea_id in sorted(subfonte['alineas'].keys()):
                        alinea = subfonte['alineas'][alinea_id]
                        self._calcular_variacoes(alinea)
                        lista_saida.append(alinea)

    def _calcular_variacoes(self, item):
        receita_atual = item.get('receita_atual', 0) or 0
        receita_anterior = item.get('receita_anterior', 0) or 0
        item['variacao_absoluta'] = receita_atual - receita_anterior
        if receita_anterior != 0:
            item['variacao_percentual'] = (item['variacao_absoluta'] / abs(receita_anterior)) * 100
        else:
            item['variacao_percentual'] = 100.0 if receita_atual != 0 else 0.0

    def _calcular_total_geral(self, dados):
        total = {'id': 'total', 'codigo': '', 'descricao': 'TOTAL GERAL', 'nivel': -1, 'classes': 'nivel--1', **self._valores_zerados()}
        for item in dados:
            if item.get('nivel') == 0:
                for campo in ['previsao_inicial', 'previsao_atualizada', 'receita_atual', 'receita_anterior']: total[campo] += item.get(campo, 0)
        self._calcular_variacoes(total)
        return total

    def _valores_zerados(self): return {'previsao_inicial': 0, 'previsao_atualizada': 0, 'receita_atual': 0, 'receita_anterior': 0}


@relatorios_bp.route('/')
def index():
    periodo = obter_periodo_referencia()
    return render_template('relatorios_orcamentarios/index.html', periodo=periodo)

@relatorios_bp.route('/balanco-orcamentario-receita')
def balanco_orcamentario_receita():
    try:
        with ConexaoBanco() as conn:
            formato = request.args.get('formato', 'html')
            periodo = obter_periodo_referencia()
            filtro_relatorio_key = request.args.get('filtro')
            processador = ProcessadorDadosReceita(conn)
            coug_selecionada = processador.coug_manager.get_coug_da_url()
            dados = processador.buscar_dados_balanco(periodo['mes'], periodo['ano'], coug_selecionada, filtro_relatorio_key)
            if formato == 'excel':
                return exportar_excel_balanco(dados, periodo, coug_selecionada, processador.coug_manager, filtro_relatorio_key)
            
            # As chamadas a seguir precisam de uma conexão ativa
            conn_para_modulos = conn
            
            comparativo_mensal = gerar_comparativo_mensal(conn_para_modulos, periodo['ano'], coug_selecionada, filtro_relatorio_key)
            dados_cards = gerar_cards_unidades(conn_para_modulos, periodo['ano'], periodo['mes'], filtro_relatorio_key)
            resumo = gerar_resumo_executivo(dados)
            cougs = processador.coug_manager.listar_cougs_com_movimento([get_filtro_conta('RECEITA_LIQUIDA')])
            nome_coug = processador.coug_manager.get_nome_coug(coug_selecionada) if coug_selecionada else "Consolidado"

            chart_data_categorias = [{"label": item['descricao'], "value": item['receita_atual']} for item in dados if item.get('nivel') == 0 and item.get('receita_atual', 0) > 0]
            chart_data_origens = [{"label": item['descricao'], "value": item['receita_atual']} for item in dados if item.get('nivel') == 1 and item.get('receita_atual', 0) > 0]
            
            filtro_info = FILTROS_RELATORIO_ESPECIAIS.get(filtro_relatorio_key, {'descricao': 'Todas as Receitas'})
            titulo_comparativo = f"Comparativo Mensal Acumulado - {filtro_info['descricao']}"
            
            return render_template(
                'relatorios_orcamentarios/balanco_orcamentario_receita.html',
                dados=dados, periodo=periodo, cougs=cougs, coug_selecionada=coug_selecionada,
                nome_coug=nome_coug, chart_data_categorias=chart_data_categorias,
                chart_data_origens=chart_data_origens, resumo_executivo=resumo,
                data_geracao=datetime.now().strftime('%d/%m/%Y %H:%M'),
                filtro_ativo=filtro_relatorio_key, filtro_descricao=filtro_info['descricao'],
                comparativo_mensal=comparativo_mensal, titulo_comparativo=titulo_comparativo,
                dados_cards=dados_cards, gerar_botao_lancamentos=gerar_botao_lancamentos
            )
    except Exception as e:
        traceback.print_exc()
        return render_template('erro.html', mensagem=f"Erro inesperado ao gerar relatório: {e}")

@relatorios_bp.route('/api/lancamentos', methods=['GET'])
def api_lancamentos():
    try:
        with ConexaoBanco() as conn:
            resultado = processar_requisicao_lancamentos(conn, request.args)
            if 'erro' in resultado:
                return jsonify(resultado), 400
            return jsonify(resultado)
    except Exception as e:
        traceback.print_exc()
        return jsonify({"erro": str(e)}), 500

@relatorios_bp.route('/api/relatorio-receita-fonte')
def api_relatorio_receita_fonte():
    try:
        with ConexaoBanco() as conn:
            resultado = gerar_relatorio_receita_fonte(
                conn=conn,
                tipo=request.args.get('tipo', 'receita'),
                ano=int(request.args.get('ano')),
                mes=int(request.args.get('mes')),
                coug=request.args.get('coug') or None,
                filtro_relatorio_key=request.args.get('filtro')
            )
            return jsonify(resultado)
    except Exception as e:
        traceback.print_exc()
        return jsonify({"erro": str(e)}), 500

@relatorios_bp.route('/api/lancamentos-receita-fonte')
def api_lancamentos_receita_fonte():
    try:
        with ConexaoBanco() as conn:
            ano = int(request.args.get('ano'))
            mes = int(request.args.get('mes'))
            coug = request.args.get('coug')
            coalinea = request.args.get('coalinea')
            cofonte = request.args.get('cofonte')
            valor_relatorio = float(request.args.get('valor_relatorio', 0.0))

            if not all([ano, mes, coug, coalinea]):
                return jsonify({"erro": "Parâmetros obrigatórios faltando (ano, mes, coug, coalinea)"}), 400
            
            # CORREÇÃO: Adiciona type casts para PostgreSQL
            if get_db_environment() == 'postgres':
                query_sql = f"""
                    SELECT 
                        cocontacontabil, coug, nudocumento, coevento, indebitocredito, valancamento
                    FROM lancamentos
                    WHERE coexercicio::integer = %s
                      AND inmes::integer <= %s
                      AND cougcontab = %s
                      AND coalinea = %s
                      AND ({get_filtro_conta('RECEITA_LIQUIDA')})
                """
            else:
                query_sql = f"""
                    SELECT 
                        cocontacontabil, coug, nudocumento, coevento, indebitocredito, valancamento
                    FROM lancamentos
                    WHERE coexercicio = ?
                      AND inmes <= ?
                      AND cougcontab = ?
                      AND coalinea = ?
                      AND ({get_filtro_conta('RECEITA_LIQUIDA')})
                """
            
            query_params = [ano, mes, coug, coalinea]
            if cofonte:
                if get_db_environment() == 'postgres':
                    query_sql += " AND cofonte = %s"
                else:
                    query_sql += " AND cofonte = ?"
                query_params.append(cofonte)
            
            query_sql += " ORDER BY nudocumento, coevento"
            
            query_adaptada = adaptar_query(query_sql)
            
            if get_db_environment() == 'postgres':
                cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            else:
                cursor = conn.cursor()
            
            cursor.execute(query_adaptada, query_params)
            
            lancamentos = [dict(row) for row in cursor.fetchall()]

            total_liquido = sum(l['valancamento'] if l['indebitocredito'] == 'C' else -l['valancamento'] for l in lancamentos)

            html = ""
            if lancamentos:
                html += f"""
                <div class="modal-info-container">
                    <div class="valor-apurado-info">
                        <strong>Valor Apurado no Relatório:</strong> {formatar_moeda(valor_relatorio)}
                    </div>
                </div>
                <div class="table-container">
                    <table class="lancamentos-table">
                        <thead>
                            <tr><th>Conta Contábil</th><th>UG Emitente</th><th>Documento</th><th>Evento</th><th>D/C</th><th>Valor</th></tr>
                        </thead>
                        <tbody>
                """
                for lanc in lancamentos:
                    html += f"""
                        <tr>
                            <td>{lanc['cocontacontabil']}</td>
                            <td>{lanc['coug']}</td>
                            <td>{lanc['nudocumento']}</td>
                            <td>{lanc['coevento']}</td>
                            <td>{lanc['indebitocredito']}</td>
                            <td>{formatar_moeda(lanc['valancamento'])}</td>
                        </tr>
                    """
                html += f"""
                        </tbody>
                        <tfoot>
                            <tr>
                                <td colspan="5">Total Líquido dos Lançamentos:</td>
                                <td>{formatar_moeda(total_liquido)}</td>
                            </tr>
                        </tfoot>
                    </table>
                </div>
                """
                resultado = {'tem_dados': True, 'html_tabela': html}
            else:
                resultado = {'tem_dados': False, 'html_tabela': '<div class="modal-info-container"><p>Nenhum lançamento encontrado para este item.</p></div>'}

            return jsonify(resultado)

    except Exception as e:
        traceback.print_exc()
        return jsonify({"erro": str(e)}), 500

@relatorios_bp.app_template_filter('formatar_moeda')
def filter_formatar_moeda(valor):
    return formatar_moeda(valor)

@relatorios_bp.app_template_filter('formatar_percentual')
def filter_formatar_percentual(valor):
    return formatar_percentual(valor/100 if valor is not None else 0, casas_decimais=2)