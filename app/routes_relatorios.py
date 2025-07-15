# app/routes_relatorios.py
"""Rotas para os relatórios do sistema orçamentário - Versão com Filtro Dinâmico"""

from flask import Blueprint, render_template, request, send_file, jsonify
import sqlite3
import os
from datetime import datetime
import pandas as pd
from io import BytesIO

# Importa módulos do sistema
from app.modulos.periodo import obter_periodo_referencia
from app.modulos.formatacao import formatar_moeda, formatar_percentual
# Importa as novas regras de filtro
from app.modulos.regras_contabeis_receita import get_filtro_conta, FILTROS_RELATORIO_ESPECIAIS
from app.modulos.coug_manager import COUGManager

# Cria o Blueprint
relatorios_bp = Blueprint('relatorios', __name__, url_prefix='/relatorios')

def gerar_resumo_executivo(dados):
    """Calcula métricas e insights para o resumo executivo a partir dos dados do relatório."""
    if not dados or len(dados) <= 1:
        return None

    resumo = {}
    try:
        total_geral = next(item for item in dados if item['id'] == 'total')
        resumo['total_geral'] = {
            'receita_2025': total_geral.get('receita_atual', 0),
            'receita_2024': total_geral.get('receita_anterior', 0),
            'variacao_abs': total_geral.get('variacao_absoluta', 0),
            'variacao_pct': total_geral.get('variacao_percentual', 0)
        }

        categorias = [d for d in dados if d.get('nivel') == 0]
        resumo['contagem_categorias'] = len(categorias)
        resumo['contagem_detalhamentos'] = len([d for d in dados if d.get('nivel') == 3])

        if categorias:
            cat_principal = max(categorias, key=lambda item: item['receita_atual'])
            resumo['categoria_principal'] = {
                'descricao': cat_principal['descricao'],
                'valor': cat_principal['receita_atual']
            }

            itens_variacao = [d for d in dados if d.get('nivel') == 1]
            if not itens_variacao:
                 itens_variacao = categorias

            if itens_variacao:
                maior_crescimento = max(itens_variacao, key=lambda item: item['variacao_absoluta'])
                if maior_crescimento['variacao_absoluta'] > 0:
                    resumo['maior_crescimento'] = {
                        'descricao': maior_crescimento['descricao'],
                        'valor': maior_crescimento['variacao_absoluta']
                    }

                maior_queda = min(itens_variacao, key=lambda item: item['variacao_absoluta'])
                if maior_queda['variacao_absoluta'] < 0:
                    resumo['maior_queda'] = {
                        'descricao': maior_queda['descricao'],
                        'valor': maior_queda['variacao_absoluta']
                    }

        return resumo
    except (StopIteration, KeyError):
        return None


class ConexaoBanco:
    @staticmethod
    def get_caminho_banco(nome_banco):
        base_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'dados', 'db')
        return os.path.join(base_path, nome_banco)
    
    @staticmethod
    def conectar_completo(nome_banco_principal='banco_saldo_receita.db'):
        caminho_principal = ConexaoBanco.get_caminho_banco(nome_banco_principal)
        if not os.path.exists(caminho_principal):
            raise FileNotFoundError(f"Banco de dados principal não encontrado: {caminho_principal}")
        
        conn = sqlite3.connect(caminho_principal)
        conn.row_factory = sqlite3.Row

        caminho_dimensoes = ConexaoBanco.get_caminho_banco('banco_dimensoes.db')
        if os.path.exists(caminho_dimensoes):
            try:
                conn.execute(f"ATTACH DATABASE '{caminho_dimensoes}' AS dimensoes")
            except Exception as e:
                print(f"Aviso: Não foi possível anexar banco de dimensões: {e}")

        caminho_lancamentos = ConexaoBanco.get_caminho_banco('banco_lancamento_receita.db')
        if os.path.exists(caminho_lancamentos):
            try:
                conn.execute(f"ATTACH DATABASE '{caminho_lancamentos}' AS lancamentos_db")
            except Exception as e:
                print(f"Aviso: Não foi possível anexar banco de lançamentos: {e}")
        else:
             print(f"Aviso: Banco de lançamentos não encontrado em {caminho_lancamentos}. O detalhamento não funcionará.")

        return conn
    
    @staticmethod
    def verificar_estrutura(conn):
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tabelas_principais = [row[0] for row in cursor.fetchall()]
        colunas_fato = []
        if 'fato_saldos' in tabelas_principais:
            cursor.execute("PRAGMA table_info(fato_saldos)")
            colunas_fato = [row[1] for row in cursor.fetchall()]
        
        tem_dimensoes = False
        try:
            cursor.execute("SELECT name FROM dimensoes.sqlite_master WHERE type='table' LIMIT 1")
            tem_dimensoes = len(cursor.fetchall()) > 0
        except: pass
        
        tem_lancamentos = False
        try:
            cursor.execute("SELECT name FROM lancamentos_db.sqlite_master WHERE type='table' and name='lancamentos' LIMIT 1")
            tem_lancamentos = len(cursor.fetchall()) > 0
        except: pass

        return {
            'colunas_fato_saldos': colunas_fato, 
            'tem_dimensoes': tem_dimensoes,
            'tem_lancamentos': tem_lancamentos
        }

class ProcessadorDadosReceita:
    def __init__(self, conn):
        self.conn = conn
        self.estrutura = ConexaoBanco.verificar_estrutura(conn)
        self.coug_manager = COUGManager(conn)
    
    def buscar_dados_balanco(self, mes, ano, coug=None, filtro_relatorio_key=None):
        query = self._query_agregada(mes, ano, coug, filtro_relatorio_key)
        try:
            cursor = self.conn.execute(query)
            resultados = cursor.fetchall()
            return self._processar_resultados_agregados(resultados)
        except Exception as e:
            print(f"Erro ao buscar dados agregados: {e}")
            import traceback
            traceback.print_exc()
            return self._dados_exemplo()

    def _query_agregada(self, mes, ano, coug=None, filtro_relatorio_key=None):
        filtro_coug = self.coug_manager.aplicar_filtro_query("fs", coug)
        
        filtro_dinamico = ""
        if filtro_relatorio_key and filtro_relatorio_key in FILTROS_RELATORIO_ESPECIAIS:
            regra = FILTROS_RELATORIO_ESPECIAIS[filtro_relatorio_key]
            campo = regra['campo_filtro']
            valores_str = ", ".join([f"'{v}'" for v in regra['valores']])
            filtro_dinamico = f"AND fs.{campo} IN ({valores_str})"
        
        if not self.estrutura['tem_dimensoes']: return ""

        query = f"""
        WITH dados_agregados AS (
            SELECT 
                fs.CATEGORIARECEITA, COALESCE(cat.NOCATEGORIARECEITA, 'Cat ' || fs.CATEGORIARECEITA) as nome_categoria,
                fs.COFONTERECEITA, COALESCE(ori.NOFONTERECEITA, 'Fonte ' || fs.COFONTERECEITA) as nome_fonte,
                fs.COSUBFONTERECEITA, COALESCE(esp.NOSUBFONTERECEITA, 'Subfonte ' || fs.COSUBFONTERECEITA) as nome_subfonte,
                fs.COALINEA, COALESCE(ali.NOALINEA, 'Alinea ' || fs.COALINEA) as nome_alinea,
                fs.COEXERCICIO, fs.INMES,
                SUM(CASE WHEN {get_filtro_conta('PREVISAO_INICIAL_LIQUIDA')} THEN COALESCE(fs.saldo_contabil, 0) ELSE 0 END) as previsao_inicial,
                SUM(CASE WHEN {get_filtro_conta('PREVISAO_ATUALIZADA_LIQUIDA')} THEN COALESCE(fs.saldo_contabil, 0) ELSE 0 END) as previsao_atualizada,
                SUM(CASE WHEN {get_filtro_conta('RECEITA_LIQUIDA')} THEN COALESCE(fs.saldo_contabil, 0) ELSE 0 END) as receita_liquida
            FROM fato_saldos fs
            LEFT JOIN dimensoes.categorias cat ON fs.CATEGORIARECEITA = cat.COCATEGORIARECEITA
            LEFT JOIN dimensoes.origens ori ON fs.COFONTERECEITA = ori.COFONTERECEITA
            LEFT JOIN dimensoes.especies esp ON fs.COSUBFONTERECEITA = esp.COSUBFONTERECEITA
            LEFT JOIN dimensoes.alineas ali ON fs.COALINEA = ali.COALINEA
            WHERE 1=1 {filtro_coug} {filtro_dinamico}
            GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
        )
        SELECT 
            CATEGORIARECEITA, nome_categoria,
            COFONTERECEITA, nome_fonte,
            COSUBFONTERECEITA, nome_subfonte,
            COALINEA, nome_alinea,
            SUM(CASE WHEN COEXERCICIO = {ano} THEN previsao_inicial ELSE 0 END) as previsao_inicial,
            SUM(CASE WHEN COEXERCICIO = {ano} THEN previsao_atualizada ELSE 0 END) as previsao_atualizada,
            SUM(CASE WHEN COEXERCICIO = {ano} AND INMES <= {mes} THEN receita_liquida ELSE 0 END) as receita_atual,
            SUM(CASE WHEN COEXERCICIO = {ano-1} AND INMES <= {mes} THEN receita_liquida ELSE 0 END) as receita_anterior
        FROM dados_agregados
        WHERE COEXERCICIO IN ({ano}, {ano-1})
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
        HAVING (ABS(previsao_inicial) + ABS(previsao_atualizada) + ABS(receita_atual) + ABS(receita_anterior)) > 0.01
        ORDER BY CATEGORIARECEITA, COFONTERECEITA, COSUBFONTERECEITA, COALINEA
        """
        return query

    def _processar_resultados_agregados(self, resultados):
        if not resultados: return self._dados_exemplo()
        
        dados_processados = []
        categorias = {}
        
        for row in resultados:
            cat_id, fonte_id, subfonte_id, alinea_id = row['CATEGORIARECEITA'], row['COFONTERECEITA'], row['COSUBFONTERECEITA'], row['COALINEA']
            
            if cat_id not in categorias:
                categorias[cat_id] = {'id': f'cat-{cat_id}', 'codigo': cat_id, 'descricao': row['nome_categoria'], 'nivel': 0, 'classes': 'nivel-0 parent-row', 'fontes': {}, **self._valores_base()}
            cat = categorias[cat_id]
            
            if fonte_id not in cat['fontes']:
                cat['fontes'][fonte_id] = {'id': f'fonte-{cat_id}-{fonte_id}', 'codigo': fonte_id, 'descricao': row['nome_fonte'], 'nivel': 1, 'classes': 'nivel-1 parent-row', 'subfontes': {}, **self._valores_base()}
            fonte = cat['fontes'][fonte_id]

            if subfonte_id not in fonte['subfontes']:
                fonte['subfontes'][subfonte_id] = {'id': f'sub-{cat_id}-{fonte_id}-{subfonte_id}', 'codigo': subfonte_id, 'descricao': row['nome_subfonte'], 'nivel': 2, 'classes': 'nivel-2 parent-row child-row', 'alineas': {}, **self._valores_base()}
            subfonte = fonte['subfontes'][subfonte_id]

            if alinea_id and alinea_id not in subfonte['alineas']:
                alinea_desc = f"{row['COALINEA']} - {row['nome_alinea']}"
                subfonte['alineas'][alinea_id] = {
                    'id': f'ali-{cat_id}-{fonte_id}-{subfonte_id}-{alinea_id}', 'codigo': alinea_id, 'descricao': alinea_desc, 'nivel': 3, 'classes': 'nivel-3 child-row',
                    'previsao_inicial': row['previsao_inicial'] or 0, 'previsao_atualizada': row['previsao_atualizada'] or 0,
                    'receita_atual': row['receita_atual'] or 0, 'receita_anterior': row['receita_anterior'] or 0,
                    'tem_lancamentos': self.estrutura['tem_lancamentos'] and (row['receita_atual'] != 0 or row['receita_anterior'] != 0),
                    'params_lancamentos': {
                        'cat_id': cat_id, 'fonte_id': fonte_id, 'subfonte_id': subfonte_id, 'alinea_id': alinea_id
                    }
                }

        for cat in categorias.values():
            for fonte in cat['fontes'].values():
                for subfonte in fonte['subfontes'].values():
                    for alinea in subfonte['alineas'].values():
                        for key in ['previsao_inicial', 'previsao_atualizada', 'receita_atual', 'receita_anterior']:
                            subfonte[key] += alinea[key]
                            fonte[key] += alinea[key]
                            cat[key] += alinea[key]

        totais = self._valores_base()
        for cat in categorias.values():
            for key in totais.keys():
                totais[key] += cat.get(key, 0)

        def calc_variacao(item):
            item['variacao_absoluta'] = item['receita_atual'] - item['receita_anterior']
            item['variacao_percentual'] = (item['variacao_absoluta'] / item['receita_anterior'] * 100) if item.get('receita_anterior') and item.get('receita_anterior') != 0 else 0
            return item

        for cat_id in sorted(categorias.keys()):
            cat = categorias[cat_id]
            cat['classes'] = 'nivel-0 parent-row'
            dados_processados.append({k: v for k, v in calc_variacao(cat).items() if k != 'fontes'})
            for fonte_id in sorted(cat['fontes'].keys()):
                fonte = cat['fontes'][fonte_id]
                fonte['classes'] = 'nivel-1 parent-row child-row'
                dados_processados.append({k: v for k, v in calc_variacao(fonte).items() if k != 'subfontes'})
                for subfonte_id in sorted(fonte['subfontes'].keys()):
                    subfonte = fonte['subfontes'][subfonte_id]
                    subfonte['classes'] = 'nivel-2 parent-row child-row'
                    dados_processados.append({k: v for k, v in calc_variacao(subfonte).items() if k != 'alineas'})
                    for alinea_id in sorted(subfonte['alineas'].keys()):
                        alinea = subfonte['alineas'][alinea_id]
                        dados_processados.append(calc_variacao(alinea))

        dados_processados.append(calc_variacao({'id': 'total', 'codigo': '', 'descricao': 'TOTAL GERAL', 'nivel': -1, 'classes': 'nivel--1', **totais}))
        return dados_processados

    def _valores_base(self):
        return {'previsao_inicial': 0, 'previsao_atualizada': 0, 'receita_atual': 0, 'receita_anterior': 0}

    def _dados_exemplo(self):
        return [{'id': 'total', 'codigo': '', 'descricao': 'NENHUM DADO ENCONTRADO', 'nivel': -1, 'classes': 'nivel--1', 'previsao_inicial': 0, 'previsao_atualizada': 0, 'receita_atual': 0, 'receita_anterior': 0, 'variacao_absoluta': 0, 'variacao_percentual': 0}]


@relatorios_bp.route('/')
def index():
    periodo = obter_periodo_referencia()
    return render_template('relatorios_orcamentarios/index.html', periodo=periodo)

@relatorios_bp.route('/balanco-orcamentario-receita')
def balanco_orcamentario_receita():
    try:
        conn = ConexaoBanco.conectar_completo()
        
        formato = request.args.get('formato', 'html')
        periodo = obter_periodo_referencia()
        
        coug_manager = COUGManager(conn)
        coug_selecionada = coug_manager.get_coug_da_url()

        # Captura a chave do filtro dinâmico da URL (ex: ?filtro=contribuicoes)
        filtro_relatorio_key = request.args.get('filtro', None)
        
        processador = ProcessadorDadosReceita(conn)
        # Passa a chave do filtro para o método de busca
        dados = processador.buscar_dados_balanco(periodo['mes'], periodo['ano'], coug_selecionada, filtro_relatorio_key)
        
        resumo = gerar_resumo_executivo(dados)
        
        if formato == 'excel':
            conn.close()
            return exportar_excel_balanco(dados, periodo, coug_selecionada, coug_manager, filtro_relatorio_key)

        filtros_contas_receita = [ get_filtro_conta('RECEITA_LIQUIDA') ]
        cougs = coug_manager.listar_cougs_com_movimento(filtros_contas_receita)
        conn.close()

        chart_data_categorias = [ {"label": item['descricao'], "value": item['receita_atual']} for item in dados if item['nivel'] == 0 and item.get('receita_atual', 0) > 0 ]
        chart_data_origens = [ {"label": item['descricao'], "value": item['receita_atual']} for item in dados if item['nivel'] == 1 and item.get('receita_atual', 0) > 0 ]
        
        return render_template(
            'relatorios_orcamentarios/balanco_orcamentario_receita.html', 
            dados=dados, 
            periodo=periodo, 
            cougs=cougs, 
            coug_selecionada=coug_selecionada, 
            chart_data_categorias=chart_data_categorias, 
            chart_data_origens=chart_data_origens,
            resumo_executivo=resumo,
            data_geracao=datetime.now().strftime('%d/%m/%Y %H:%M'),
            # Passa a chave do filtro ativo para o template
            filtro_ativo=filtro_relatorio_key
        )
    except Exception as e:
        import traceback
        traceback.print_exc()
        return render_template('erro.html', mensagem=f"Erro ao gerar relatório: {str(e)}")

@relatorios_bp.route('/balanco-orcamentario-receita/lancamentos')
def buscar_lancamentos():
    try:
        ano = request.args.get('ano', type=int)
        mes = request.args.get('mes', type=int)
        coug = request.args.get('coug', '')
        cat_id = request.args.get('cat_id')
        fonte_id = request.args.get('fonte_id')
        subfonte_id = request.args.get('subfonte_id')
        alinea_id = request.args.get('alinea_id')
        ano_busca = ano
        
        query = f"""
            SELECT COUG, COCONTACONTABIL, NUDOCUMENTO, COEVENTO, INDEBITOCREDITO, VALANCAMENTO
            FROM lancamentos_db.lancamentos
            WHERE COEXERCICIO = ?
              AND INMES <= ?
              AND CATEGORIARECEITA = ?
              AND COFONTERECEITA = ?
              AND COSUBFONTERECEITA = ?
              AND COALINEA = ?
              AND COCONTACONTABIL BETWEEN '621200000' AND '621399999'
              AND NUDOCUMENTO LIKE '{ano_busca}%'
        """
        params = [ano_busca, mes, cat_id, fonte_id, subfonte_id, alinea_id]

        if coug:
            query += " AND COUGCONTAB = ?"
            params.append(coug)
        
        query += " ORDER BY COEVENTO, VALANCAMENTO DESC"

        conn = ConexaoBanco.conectar_completo()
        cursor = conn.execute(query, params)
        lancamentos = [dict(row) for row in cursor.fetchall()]
        conn.close()

        return jsonify(lancamentos)
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"erro": str(e)}), 500


def exportar_excel_balanco(dados, periodo, coug_selecionada, coug_manager, filtro_relatorio_key=None):
    rows = []
    for item in dados:
        if item.get('nivel', -2) >= -1:
            indent_level = item.get('nivel', 0)
            if indent_level < 0: indent_level = 0
            indent = '    ' * indent_level
            row_data = { 'Código': item.get('codigo', ''), 'Descrição': indent + item.get('descricao', ''), f'Previsão Inicial {periodo["ano"]}': item.get('previsao_inicial', 0), f'Previsão Atualizada {periodo["ano"]}': item.get('previsao_atualizada', 0), f'Receita Realizada {periodo["mes"]}/{periodo["ano"]}': item.get('receita_atual', 0), f'Receita Realizada {periodo["mes"]}/{periodo["ano"]-1}': item.get('receita_anterior', 0), 'Variação Absoluta': item.get('variacao_absoluta', 0), 'Variação %': item.get('variacao_percentual', 0) / 100 }
            rows.append(row_data)
    df = pd.DataFrame(rows)
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='Balanço Orçamentário', index=False, float_format="%.2f")
        worksheet = writer.sheets['Balanço Orçamentário']
        money_format, percent_format = 'R$ #,##0.00', '0.00%'
        worksheet.column_dimensions['A'].width, worksheet.column_dimensions['B'].width = 15, 60
        for col_letter in ['C', 'D', 'E', 'F', 'G']:
            worksheet.column_dimensions[col_letter].width = 22
            for cell in worksheet[col_letter]: cell.number_format = money_format
        worksheet.column_dimensions['H'].width = 15
        for cell in worksheet['H']: cell.number_format = percent_format
    output.seek(0)
    suffix = coug_manager.get_sufixo_arquivo(coug_selecionada)
    filtro_suffix = f"_{filtro_relatorio_key}" if filtro_relatorio_key else ""
    filename = f'balanco_orcamentario_receita{suffix}{filtro_suffix}_{periodo["ano"]}_{periodo["mes"]:02d}.xlsx'
    return send_file(output, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', as_attachment=True, download_name=filename)

@relatorios_bp.app_template_filter('formatar_moeda')
def filter_formatar_moeda(valor):
    return formatar_moeda(valor)

@relatorios_bp.app_template_filter('formatar_percentual')
def filter_formatar_percentual(valor):
    return formatar_percentual(valor/100 if valor else 0)
