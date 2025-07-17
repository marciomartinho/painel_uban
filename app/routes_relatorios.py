# app/routes_relatorios.py
"""
Rotas para os relatórios do sistema orçamentário
Versão com Filtro Dinâmico, Comparativo Mensal e Cards de Unidades Gestoras
"""

from flask import Blueprint, render_template, request, send_file, jsonify, Response
import sqlite3
import os
from datetime import datetime
import pandas as pd
from io import BytesIO
import traceback

# Importa módulos do sistema
from app.modulos.periodo import obter_periodo_referencia
from app.modulos.formatacao import formatar_moeda, formatar_percentual
from app.modulos.regras_contabeis_receita import get_filtro_conta, FILTROS_RELATORIO_ESPECIAIS
from app.modulos.coug_manager import COUGManager
from app.modulos.comparativo_mensal import gerar_comparativo_mensal
from app.modulos.cards_unidades_gestoras import gerar_cards_unidades
from app.modulos.relatorio_receita_fonte import gerar_relatorio_receita_fonte
from app.modulos.modal_lancamentos import processar_requisicao_lancamentos, gerar_botao_lancamentos
from app.modulos.exportador_html import exportar_relatorio_html

# Cria o Blueprint
relatorios_bp = Blueprint('relatorios', __name__, url_prefix='/relatorios')


class ConexaoBanco:
    """Gerenciador de conexões com o banco de dados"""
    
    @staticmethod
    def get_caminho_banco(nome_banco):
        """Retorna o caminho completo do banco de dados"""
        base_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'dados', 'db')
        return os.path.join(base_path, nome_banco)
    
    @staticmethod
    def conectar_completo(nome_banco_principal='banco_saldo_receita.db'):
        """
        Conecta ao banco principal e anexa bancos auxiliares
        
        Args:
            nome_banco_principal: Nome do arquivo do banco principal
            
        Returns:
            Conexão SQLite com bancos anexados
            
        Raises:
            FileNotFoundError: Se o banco principal não for encontrado
        """
        caminho_principal = ConexaoBanco.get_caminho_banco(nome_banco_principal)
        if not os.path.exists(caminho_principal):
            raise FileNotFoundError(f"Banco de dados principal não encontrado: {caminho_principal}")
        
        conn = sqlite3.connect(caminho_principal)
        conn.row_factory = sqlite3.Row
        
        # Anexa banco de dimensões
        caminho_dimensoes = ConexaoBanco.get_caminho_banco('banco_dimensoes.db')
        if os.path.exists(caminho_dimensoes):
            try:
                conn.execute(f"ATTACH DATABASE '{caminho_dimensoes}' AS dimensoes")
            except Exception as e:
                print(f"Aviso: Não foi possível anexar banco de dimensões: {e}")
        
        # Anexa banco de lançamentos
        caminho_lancamentos = ConexaoBanco.get_caminho_banco('banco_lancamento_receita.db')
        if os.path.exists(caminho_lancamentos):
            try:
                conn.execute(f"ATTACH DATABASE '{caminho_lancamentos}' AS lancamentos_db")
            except Exception as e:
                print(f"Aviso: Não foi possível anexar banco de lançamentos: {e}")
        else:
            print(f"Aviso: Banco de lançamentos não encontrado. O detalhamento não funcionará.")
        
        return conn
    
    @staticmethod
    def verificar_estrutura(conn):
        """
        Verifica a estrutura dos bancos anexados
        
        Args:
            conn: Conexão SQLite
            
        Returns:
            Dict com informações sobre a estrutura
        """
        cursor = conn.cursor()
        
        # Verifica tabelas do banco principal
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tabelas_principais = [row[0] for row in cursor.fetchall()]
        
        # Verifica colunas da fato_saldos
        colunas_fato = []
        if 'fato_saldos' in tabelas_principais:
            cursor.execute("PRAGMA table_info(fato_saldos)")
            colunas_fato = [row[1] for row in cursor.fetchall()]
        
        # Verifica se dimensões foram anexadas
        tem_dimensoes = False
        try:
            cursor.execute("SELECT 1 FROM dimensoes.sqlite_master LIMIT 1")
            tem_dimensoes = True
        except:
            pass
        
        # Verifica se lançamentos foram anexados
        tem_lancamentos = False
        try:
            cursor.execute("SELECT 1 FROM lancamentos_db.lancamentos LIMIT 1")
            tem_lancamentos = True
        except:
            pass
        
        return {
            'colunas_fato_saldos': colunas_fato,
            'tem_dimensoes': tem_dimensoes,
            'tem_lancamentos': tem_lancamentos
        }


class ProcessadorDadosReceita:
    """Processa dados para o relatório de balanço orçamentário"""
    
    def __init__(self, conn):
        self.conn = conn
        self.estrutura = ConexaoBanco.verificar_estrutura(conn)
        self.coug_manager = COUGManager(conn)
    
    def buscar_dados_balanco(self, mes, ano, coug=None, filtro_relatorio_key=None):
        """
        Busca dados agregados do balanço orçamentário
        
        Args:
            mes: Mês de referência
            ano: Ano de referência
            coug: Código da unidade gestora (opcional)
            filtro_relatorio_key: Chave do filtro especial (opcional)
            
        Returns:
            Lista de dados processados hierarquicamente
        """
        if not self.estrutura['tem_dimensoes']:
            print("Aviso: Banco de dimensões não disponível")
            return self._dados_exemplo()
        
        query = self._montar_query_agregada(mes, ano, coug, filtro_relatorio_key)
        
        try:
            cursor = self.conn.execute(query)
            resultados = cursor.fetchall()
            return self._processar_resultados_agregados(resultados)
        except Exception as e:
            print(f"Erro ao buscar dados agregados: {e}")
            traceback.print_exc()
            return self._dados_exemplo()
    
    def _montar_query_agregada(self, mes, ano, coug=None, filtro_relatorio_key=None):
        """Monta a query SQL para buscar dados agregados"""
        # Filtro de COUG
        filtro_coug = self.coug_manager.aplicar_filtro_query("fs", coug)
        
        # Filtro dinâmico de tipo de receita
        filtro_dinamico = ""
        if filtro_relatorio_key and filtro_relatorio_key in FILTROS_RELATORIO_ESPECIAIS:
            regra = FILTROS_RELATORIO_ESPECIAIS[filtro_relatorio_key]
            campo = regra['campo_filtro']
            valores_str = ", ".join([f"'{v}'" for v in regra['valores']])
            filtro_dinamico = f"AND fs.{campo} IN ({valores_str})"
        
        return f"""
        WITH dados_agregados AS (
            SELECT 
                fs.CATEGORIARECEITA,
                COALESCE(cat.NOCATEGORIARECEITA, 'Categoria ' || fs.CATEGORIARECEITA) as nome_categoria,
                fs.COFONTERECEITA,
                COALESCE(ori.NOFONTERECEITA, 'Fonte ' || fs.COFONTERECEITA) as nome_fonte,
                fs.COSUBFONTERECEITA,
                COALESCE(esp.NOSUBFONTERECEITA, 'Subfonte ' || fs.COSUBFONTERECEITA) as nome_subfonte,
                fs.COALINEA,
                COALESCE(ali.NOALINEA, 'Alínea ' || fs.COALINEA) as nome_alinea,
                fs.COEXERCICIO,
                fs.INMES,
                SUM(CASE 
                    WHEN {get_filtro_conta('PREVISAO_INICIAL_LIQUIDA')} 
                    THEN COALESCE(fs.saldo_contabil, 0) 
                    ELSE 0 
                END) as previsao_inicial,
                SUM(CASE 
                    WHEN {get_filtro_conta('PREVISAO_ATUALIZADA_LIQUIDA')} 
                    THEN COALESCE(fs.saldo_contabil, 0) 
                    ELSE 0 
                END) as previsao_atualizada,
                SUM(CASE 
                    WHEN {get_filtro_conta('RECEITA_LIQUIDA')} 
                    THEN COALESCE(fs.saldo_contabil, 0) 
                    ELSE 0 
                END) as receita_liquida
            FROM fato_saldos fs
            LEFT JOIN dimensoes.categorias cat ON fs.CATEGORIARECEITA = cat.COCATEGORIARECEITA
            LEFT JOIN dimensoes.origens ori ON fs.COFONTERECEITA = ori.COFONTERECEITA
            LEFT JOIN dimensoes.especies esp ON fs.COSUBFONTERECEITA = esp.COSUBFONTERECEITA
            LEFT JOIN dimensoes.alineas ali ON fs.COALINEA = ali.COALINEA
            WHERE 1=1 {filtro_coug} {filtro_dinamico}
            GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
        )
        SELECT 
            CATEGORIARECEITA,
            nome_categoria,
            COFONTERECEITA,
            nome_fonte,
            COSUBFONTERECEITA,
            nome_subfonte,
            COALINEA,
            nome_alinea,
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
    
    def _processar_resultados_agregados(self, resultados):
        """Processa resultados SQL em estrutura hierárquica"""
        if not resultados:
            return self._dados_exemplo()
        
        # Estrutura hierárquica para organizar os dados
        hierarquia = {}
        
        # Processa cada linha do resultado
        for row in resultados:
            self._adicionar_na_hierarquia(hierarquia, row)
        
        # Converte hierarquia em lista plana para a tabela
        dados_processados = []
        self._hierarquia_para_lista(hierarquia, dados_processados)
        
        # Adiciona linha de total geral
        total_geral = self._calcular_total_geral(dados_processados)
        dados_processados.append(total_geral)
        
        return dados_processados
    
    def _adicionar_na_hierarquia(self, hierarquia, row):
        """Adiciona uma linha de resultado na estrutura hierárquica"""
        cat_id = row['CATEGORIARECEITA']
        fonte_id = row['COFONTERECEITA']
        subfonte_id = row['COSUBFONTERECEITA']
        alinea_id = row['COALINEA']
        
        # Cria categoria se não existir
        if cat_id not in hierarquia:
            hierarquia[cat_id] = {
                'id': f'cat-{cat_id}',
                'codigo': cat_id,
                'descricao': row['nome_categoria'],
                'nivel': 0,
                'classes': 'nivel-0',
                'fontes': {},
                **self._valores_zerados()
            }
        
        categoria = hierarquia[cat_id]
        
        # Cria fonte se não existir
        if fonte_id not in categoria['fontes']:
            categoria['fontes'][fonte_id] = {
                'id': f'fonte-{cat_id}-{fonte_id}',
                'codigo': fonte_id,
                'descricao': row['nome_fonte'],
                'nivel': 1,
                'classes': 'nivel-1 parent-row',
                'subfontes': {},
                **self._valores_zerados()
            }
        
        fonte = categoria['fontes'][fonte_id]
        
        # Cria subfonte se não existir
        if subfonte_id not in fonte['subfontes']:
            fonte['subfontes'][subfonte_id] = {
                'id': f'sub-{cat_id}-{fonte_id}-{subfonte_id}',
                'codigo': subfonte_id,
                'descricao': row['nome_subfonte'],
                'nivel': 2,
                'classes': 'nivel-2 parent-row',
                'alineas': {},
                **self._valores_zerados()
            }
        
        subfonte = fonte['subfontes'][subfonte_id]
        
        # Cria alínea se existir
        if alinea_id:
            alinea_desc = f"{row['COALINEA']} - {row['nome_alinea']}"
            subfonte['alineas'][alinea_id] = {
                'id': f'ali-{cat_id}-{fonte_id}-{subfonte_id}-{alinea_id}',
                'codigo': alinea_id,
                'descricao': alinea_desc,
                'nivel': 3,
                'classes': 'nivel-3',
                'previsao_inicial': row['previsao_inicial'] or 0,
                'previsao_atualizada': row['previsao_atualizada'] or 0,
                'receita_atual': row['receita_atual'] or 0,
                'receita_anterior': row['receita_anterior'] or 0,
                'tem_lancamentos': self.estrutura['tem_lancamentos'] and (
                    row['receita_atual'] != 0 or row['receita_anterior'] != 0
                ),
                'params_lancamentos': {
                    'cat_id': cat_id,
                    'fonte_id': fonte_id,
                    'subfonte_id': subfonte_id,
                    'alinea_id': alinea_id
                }
            }
            
            # Acumula valores nos níveis superiores
            for campo in ['previsao_inicial', 'previsao_atualizada', 'receita_atual', 'receita_anterior']:
                valor = row[campo] or 0
                subfonte[campo] += valor
                fonte[campo] += valor
                categoria[campo] += valor
    
    def _hierarquia_para_lista(self, hierarquia, lista_saida):
        """Converte estrutura hierárquica em lista plana"""
        for cat_id in sorted(hierarquia.keys()):
            categoria = hierarquia[cat_id]
            self._calcular_variacoes(categoria)
            
            # Adiciona categoria (sem incluir dicionários internos)
            lista_saida.append({
                k: v for k, v in categoria.items() 
                if k != 'fontes'
            })
            
            # Processa fontes
            for fonte_id in sorted(categoria['fontes'].keys()):
                fonte = categoria['fontes'][fonte_id]
                self._calcular_variacoes(fonte)
                
                lista_saida.append({
                    k: v for k, v in fonte.items() 
                    if k != 'subfontes'
                })
                
                # Processa subfontes
                for subfonte_id in sorted(fonte['subfontes'].keys()):
                    subfonte = fonte['subfontes'][subfonte_id]
                    self._calcular_variacoes(subfonte)
                    
                    lista_saida.append({
                        k: v for k, v in subfonte.items() 
                        if k != 'alineas'
                    })
                    
                    # Processa alíneas
                    for alinea_id in sorted(subfonte['alineas'].keys()):
                        alinea = subfonte['alineas'][alinea_id]
                        self._calcular_variacoes(alinea)
                        lista_saida.append(alinea)
    
    def _calcular_variacoes(self, item):
        """Calcula variação absoluta e percentual"""
        item['variacao_absoluta'] = item['receita_atual'] - item['receita_anterior']
        
        if item['receita_anterior'] != 0:
            item['variacao_percentual'] = (
                item['variacao_absoluta'] / item['receita_anterior']
            ) * 100
        else:
            item['variacao_percentual'] = 0
    
    def _calcular_total_geral(self, dados):
        """Calcula o total geral de todos os dados"""
        total = {
            'id': 'total',
            'codigo': '',
            'descricao': 'TOTAL GERAL',
            'nivel': -1,
            'classes': 'nivel--1',
            **self._valores_zerados()
        }
        
        # Soma apenas itens de nível 0 (categorias) para evitar dupla contagem
        for item in dados:
            if item.get('nivel') == 0:
                for campo in ['previsao_inicial', 'previsao_atualizada', 'receita_atual', 'receita_anterior']:
                    total[campo] += item.get(campo, 0)
        
        self._calcular_variacoes(total)
        return total
    
    def _valores_zerados(self):
        """Retorna dicionário com valores zerados"""
        return {
            'previsao_inicial': 0,
            'previsao_atualizada': 0,
            'receita_atual': 0,
            'receita_anterior': 0
        }
    
    def _dados_exemplo(self):
        """Retorna dados de exemplo quando não há dados reais"""
        return [{
            'id': 'total',
            'codigo': '',
            'descricao': 'NENHUM DADO ENCONTRADO',
            'nivel': -1,
            'classes': 'nivel--1',
            'previsao_inicial': 0,
            'previsao_atualizada': 0,
            'receita_atual': 0,
            'receita_anterior': 0,
            'variacao_absoluta': 0,
            'variacao_percentual': 0
        }]


def gerar_resumo_executivo(dados):
    """
    Gera resumo executivo com métricas principais
    
    Args:
        dados: Lista de dados do relatório
        
    Returns:
        Dict com métricas do resumo ou None se não houver dados
    """
    if not dados or len(dados) <= 1:
        return None
    
    try:
        # Busca o total geral
        total_geral = next((item for item in dados if item['id'] == 'total'), None)
        if not total_geral:
            return None
        
        resumo = {
            'total_geral': {
                'receita_2025': total_geral.get('receita_atual', 0),
                'receita_2024': total_geral.get('receita_anterior', 0),
                'variacao_abs': total_geral.get('variacao_absoluta', 0),
                'variacao_pct': total_geral.get('variacao_percentual', 0)
            }
        }
        
        # Conta categorias e detalhamentos
        categorias = [d for d in dados if d.get('nivel') == 0]
        resumo['contagem_categorias'] = len(categorias)
        resumo['contagem_detalhamentos'] = len([d for d in dados if d.get('nivel') == 3])
        
        # Identifica categoria principal
        if categorias:
            cat_principal = max(categorias, key=lambda x: x.get('receita_atual', 0))
            resumo['categoria_principal'] = {
                'descricao': cat_principal['descricao'],
                'valor': cat_principal['receita_atual']
            }
        
        # Identifica maiores variações
        itens_com_historico = [
            d for d in dados 
            if d.get('nivel') in [0, 1] and d.get('receita_anterior', 0) > 0
        ]
        
        if itens_com_historico:
            # Maior crescimento
            crescimentos = [
                d for d in itens_com_historico 
                if d.get('variacao_absoluta', 0) > 0
            ]
            if crescimentos:
                maior_crescimento = max(crescimentos, key=lambda x: x.get('variacao_absoluta', 0))
                resumo['maior_crescimento'] = {
                    'descricao': maior_crescimento['descricao'],
                    'valor': maior_crescimento['variacao_absoluta']
                }
            
            # Maior queda
            quedas = [
                d for d in itens_com_historico 
                if d.get('variacao_absoluta', 0) < 0
            ]
            if quedas:
                maior_queda = min(quedas, key=lambda x: x.get('variacao_absoluta', 0))
                resumo['maior_queda'] = {
                    'descricao': maior_queda['descricao'],
                    'valor': maior_queda['variacao_absoluta']
                }
        
        return resumo
        
    except Exception as e:
        print(f"Erro ao gerar resumo executivo: {e}")
        return None


def exportar_excel_balanco(dados, periodo, coug_selecionada, coug_manager, filtro_relatorio_key=None):
    """
    Exporta dados do balanço para Excel
    
    Args:
        dados: Lista de dados do relatório
        periodo: Dicionário com informações do período
        coug_selecionada: Código da COUG selecionada
        coug_manager: Instância do COUGManager
        filtro_relatorio_key: Chave do filtro aplicado
        
    Returns:
        Response com arquivo Excel
    """
    rows = []
    
    # Processa cada linha de dados
    for item in dados:
        if item.get('nivel', -2) >= -1:  # Inclui todos os níveis e o total
            # Calcula indentação baseada no nível
            nivel = max(0, item.get('nivel', 0))
            indent = '    ' * nivel
            
            # Monta linha do Excel
            row = {
                'Código': item.get('codigo', ''),
                'Descrição': indent + item.get('descricao', ''),
                f'Previsão Inicial {periodo["ano"]}': item.get('previsao_inicial', 0),
                f'Previsão Atualizada {periodo["ano"]}': item.get('previsao_atualizada', 0),
                f'Receita Realizada {periodo["mes"]}/{periodo["ano"]}': item.get('receita_atual', 0),
                f'Receita Realizada {periodo["mes"]}/{periodo["ano"]-1}': item.get('receita_anterior', 0),
                'Variação Absoluta': item.get('variacao_absoluta', 0),
                'Variação %': item.get('variacao_percentual', 0) / 100  # Converte para decimal
            }
            rows.append(row)
    
    # Cria DataFrame e arquivo Excel
    df = pd.DataFrame(rows)
    output = BytesIO()
    
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='Balanço Orçamentário', index=False)
        
        # Formata a planilha
        worksheet = writer.sheets['Balanço Orçamentário']
        
        # Define larguras das colunas
        worksheet.column_dimensions['A'].width = 15  # Código
        worksheet.column_dimensions['B'].width = 60  # Descrição
        
        # Formata colunas de valores
        for col_letter in ['C', 'D', 'E', 'F', 'G']:
            worksheet.column_dimensions[col_letter].width = 22
            for cell in worksheet[col_letter][1:]:  # Pula o cabeçalho
                cell.number_format = 'R$ #,##0.00'
        
        # Formata coluna de percentual
        worksheet.column_dimensions['H'].width = 15
        for cell in worksheet['H'][1:]:  # Pula o cabeçalho
            cell.number_format = '0.00%'
    
    output.seek(0)
    
    # Monta nome do arquivo
    sufixo_coug = coug_manager.get_sufixo_arquivo(coug_selecionada)
    sufixo_filtro = f"_{filtro_relatorio_key}" if filtro_relatorio_key else ""
    filename = f'balanco_orcamentario_receita{sufixo_coug}{sufixo_filtro}_{periodo["ano"]}_{periodo["mes"]:02d}.xlsx'
    
    return send_file(
        output,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=filename
    )


# ROTAS DO BLUEPRINT

@relatorios_bp.route('/')
def index():
    """Página inicial dos relatórios"""
    periodo = obter_periodo_referencia()
    return render_template('relatorios_orcamentarios/index.html', periodo=periodo)


@relatorios_bp.route('/balanco-orcamentario-receita')
def balanco_orcamentario_receita():
    """Rota principal do balanço orçamentário da receita"""
    try:
        # Conecta ao banco
        conn = ConexaoBanco.conectar_completo()
        
        # Obtém parâmetros da requisição
        formato = request.args.get('formato', 'html')
        periodo = obter_periodo_referencia()
        filtro_relatorio_key = request.args.get('filtro', None)
        
        # Gerencia COUGs
        coug_manager = COUGManager(conn)
        coug_selecionada = coug_manager.get_coug_da_url()
        
        # Processa dados do balanço
        processador = ProcessadorDadosReceita(conn)
        dados = processador.buscar_dados_balanco(
            periodo['mes'], 
            periodo['ano'], 
            coug_selecionada, 
            filtro_relatorio_key
        )
        
        # Se formato Excel, exporta e retorna
        if formato == 'excel':
            conn.close()
            return exportar_excel_balanco(
                dados, 
                periodo, 
                coug_selecionada, 
                coug_manager, 
                filtro_relatorio_key
            )
        
        # Se formato HTML download
        if formato == 'html_download':
            # Gera componentes adicionais
            comparativo_mensal = gerar_comparativo_mensal(
                conn,
                periodo['ano'],
                coug_selecionada,
                filtro_relatorio_key
            )
            
            dados_cards = gerar_cards_unidades(
                conn,
                periodo['ano'],
                periodo['mes'],
                filtro_relatorio_key
            )
            
            # Gera resumo executivo
            resumo = gerar_resumo_executivo(dados)
            
            # Lista COUGs disponíveis
            filtros_contas_receita = [get_filtro_conta('RECEITA_LIQUIDA')]
            cougs = coug_manager.listar_cougs_com_movimento(filtros_contas_receita)
            
            # Obtém nome completo da COUG selecionada
            nome_coug = ""
            if coug_selecionada:
                for coug in cougs:
                    if coug['codigo'] == coug_selecionada:
                        nome_coug = coug['descricao_completa']
                        break
            
            # Prepara dados para gráficos
            chart_data_categorias = [
                {"label": item['descricao'], "value": item['receita_atual']}
                for item in dados 
                if item['nivel'] == 0 and item.get('receita_atual', 0) > 0
            ]
            
            chart_data_origens = [
                {"label": item['descricao'], "value": item['receita_atual']}
                for item in dados 
                if item['nivel'] == 1 and item.get('receita_atual', 0) > 0
            ]
            
            # Determina títulos e descrições baseados no filtro
            titulo_comparativo = "Comparativo Mensal Acumulado - Todas as Receitas"
            filtro_descricao = "Todas as Receitas"
            
            if filtro_relatorio_key and filtro_relatorio_key in FILTROS_RELATORIO_ESPECIAIS:
                descricao = FILTROS_RELATORIO_ESPECIAIS[filtro_relatorio_key]['descricao']
                titulo_comparativo = f"Comparativo Mensal Acumulado - {descricao}"
                filtro_descricao = descricao
            
            # Renderiza o template
            html_content = render_template(
                'relatorios_orcamentarios/balanco_orcamentario_receita.html',
                dados=dados,
                periodo=periodo,
                cougs=cougs,
                coug_selecionada=coug_selecionada,
                nome_coug=nome_coug,
                chart_data_categorias=chart_data_categorias,
                chart_data_origens=chart_data_origens,
                resumo_executivo=resumo,
                data_geracao=datetime.now().strftime('%d/%m/%Y %H:%M'),
                filtro_ativo=filtro_relatorio_key,
                filtro_descricao=filtro_descricao,
                comparativo_mensal=comparativo_mensal,
                titulo_comparativo=titulo_comparativo,
                dados_cards=dados_cards,
                gerar_botao_lancamentos=gerar_botao_lancamentos
            )
            
            conn.close()
            
            # Usa o exportador HTML
            titulo_completo = f"Balanço Orçamentário da Receita - {nome_coug if coug_selecionada else 'Consolidado'}"
            
            html_completo, nome_arquivo = exportar_relatorio_html(
                html_content,
                'balanco_orcamentario_receita',
                titulo=titulo_completo,
                periodo=periodo,
                filtros={'coug': coug_selecionada, 'filtro': filtro_relatorio_key}
            )
            
            # Retorna como download
            response = Response(html_completo, mimetype='text/html')
            response.headers['Content-Disposition'] = f'attachment; filename="{nome_arquivo}"'
            return response
        
        # Formato HTML normal (visualização)
        # Gera componentes adicionais
        comparativo_mensal = gerar_comparativo_mensal(
            conn,
            periodo['ano'],
            coug_selecionada,
            filtro_relatorio_key
        )
        
        dados_cards = gerar_cards_unidades(
            conn,
            periodo['ano'],
            periodo['mes'],
            filtro_relatorio_key
        )
        
        # Gera resumo executivo
        resumo = gerar_resumo_executivo(dados)
        
        # Lista COUGs disponíveis
        filtros_contas_receita = [get_filtro_conta('RECEITA_LIQUIDA')]
        cougs = coug_manager.listar_cougs_com_movimento(filtros_contas_receita)
        
        # Obtém nome completo da COUG selecionada
        nome_coug = ""
        if coug_selecionada:
            for coug in cougs:
                if coug['codigo'] == coug_selecionada:
                    nome_coug = coug['descricao_completa']
                    break
        
        # Prepara dados para gráficos
        chart_data_categorias = [
            {"label": item['descricao'], "value": item['receita_atual']}
            for item in dados 
            if item['nivel'] == 0 and item.get('receita_atual', 0) > 0
        ]
        
        chart_data_origens = [
            {"label": item['descricao'], "value": item['receita_atual']}
            for item in dados 
            if item['nivel'] == 1 and item.get('receita_atual', 0) > 0
        ]
        
        # Determina títulos e descrições baseados no filtro
        titulo_comparativo = "Comparativo Mensal Acumulado - Todas as Receitas"
        filtro_descricao = "Todas as Receitas"
        
        if filtro_relatorio_key and filtro_relatorio_key in FILTROS_RELATORIO_ESPECIAIS:
            descricao = FILTROS_RELATORIO_ESPECIAIS[filtro_relatorio_key]['descricao']
            titulo_comparativo = f"Comparativo Mensal Acumulado - {descricao}"
            filtro_descricao = descricao
        
        conn.close()
        
        # Renderiza template
        return render_template(
            'relatorios_orcamentarios/balanco_orcamentario_receita.html',
            dados=dados,
            periodo=periodo,
            cougs=cougs,
            coug_selecionada=coug_selecionada,
            nome_coug=nome_coug,
            chart_data_categorias=chart_data_categorias,
            chart_data_origens=chart_data_origens,
            resumo_executivo=resumo,
            data_geracao=datetime.now().strftime('%d/%m/%Y %H:%M'),
            filtro_ativo=filtro_relatorio_key,
            filtro_descricao=filtro_descricao,
            comparativo_mensal=comparativo_mensal,
            titulo_comparativo=titulo_comparativo,
            dados_cards=dados_cards,
            gerar_botao_lancamentos=gerar_botao_lancamentos  # Adiciona a função ao contexto
        )
        
    except Exception as e:
        traceback.print_exc()
        return render_template(
            'erro.html', 
            mensagem=f"Erro ao gerar relatório: {str(e)}"
        )


@relatorios_bp.route('/api/lancamentos')
def api_lancamentos():
    """API unificada para buscar lançamentos"""
    try:
        # Conecta ao banco
        conn = ConexaoBanco.conectar_completo()
        
        # Processa a requisição usando o módulo
        resultado = processar_requisicao_lancamentos(conn, request.args)
        
        conn.close()
        
        if 'erro' in resultado:
            return jsonify(resultado), 400
            
        return jsonify(resultado)
        
    except Exception as e:
        traceback.print_exc()
        return jsonify({"erro": str(e)}), 500


@relatorios_bp.route('/api/lancamentos-receita-fonte')
def api_lancamentos_receita_fonte():
    """API para buscar lançamentos do relatório receita/fonte"""
    try:
        # Conecta ao banco
        conn = ConexaoBanco.conectar_completo()
        
        # Obtém parâmetros
        ano = request.args.get('ano', type=int)
        mes = request.args.get('mes', type=int)
        coug = request.args.get('coug', '')
        coalinea = request.args.get('coalinea', '')
        cofonte = request.args.get('cofonte', '')
        valor_relatorio = request.args.get('valor_relatorio', type=float, default=0)
        
        if not all([ano, mes, coug, coalinea]):
            return jsonify({"erro": "Parâmetros obrigatórios faltando"}), 400
        
        # Query para buscar lançamentos
        query = """
        SELECT 
            l.COCONTACONTABIL,
            l.COUG,
            l.NUDOCUMENTO,
            l.COEVENTO,
            l.INDEBITOCREDITO,
            l.VALANCAMENTO
        FROM lancamentos_db.lancamentos l
        WHERE l.COEXERCICIO = ?
          AND l.INMES <= ?
          AND l.COUGCONTAB = ?
          AND l.COALINEA = ?
          AND l.COCONTACONTABIL BETWEEN '621200000' AND '621399999'
        """
        
        params = [ano, mes, coug, coalinea]
        
        # Adiciona filtro de fonte se fornecido
        if cofonte:
            query += " AND l.COFONTE = ?"
            params.append(cofonte)
        
        query += " ORDER BY l.NUDOCUMENTO, l.COEVENTO"
        
        cursor = conn.execute(query, params)
        lancamentos = []
        
        for row in cursor:
            lancamentos.append({
                'conta_contabil': row['COCONTACONTABIL'],
                'coug': row['COUG'],
                'documento': row['NUDOCUMENTO'],
                'evento': row['COEVENTO'],
                'dc': row['INDEBITOCREDITO'],
                'valor': row['VALANCAMENTO']
            })
        
        # Calcula totais
        total_debito = sum(l['valor'] for l in lancamentos if l['dc'] == 'D')
        total_credito = sum(l['valor'] for l in lancamentos if l['dc'] == 'C')
        total_liquido = total_credito - total_debito
        
        # Formata dados
        from app.modulos.formatacao import formatar_moeda
        
        # Gera HTML da tabela
        if lancamentos:
            html = f"""
            <div class="modal-info-container">
                <div class="valor-apurado-info">
                    <strong>Valor Apurado no Relatório:</strong> {formatar_moeda(valor_relatorio)}
                </div>
            </div>
            <div class="table-container">
                <table class="lancamentos-table">
                    <colgroup>
                        <col class="col-conta"><col class="col-ug"><col class="col-doc">
                        <col class="col-evento"><col class="col-dc"><col class="col-valor">
                    </colgroup>
                    <thead>
                        <tr>
                            <th>Conta Contábil</th><th>UG Emitente</th><th>Documento</th>
                            <th>Evento</th><th>D/C</th><th>Valor</th>
                        </tr>
                    </thead>
                    <tbody>
            """
            
            for lanc in lancamentos:
                html += f"""
                    <tr>
                        <td>{lanc['conta_contabil']}</td>
                        <td>{lanc['coug']}</td>
                        <td>{lanc['documento']}</td>
                        <td>{lanc['evento']}</td>
                        <td>{lanc['dc']}</td>
                        <td>{formatar_moeda(lanc['valor'])}</td>
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
            
            resultado = {
                'tem_dados': True,
                'html_tabela': html
            }
        else:
            resultado = {
                'tem_dados': False,
                'html_tabela': '<div class="modal-info-container"><p>Nenhum lançamento encontrado para este item.</p></div>'
            }
        
        conn.close()
        return jsonify(resultado)
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"erro": str(e)}), 500


# FILTROS PARA TEMPLATES

@relatorios_bp.app_template_filter('formatar_moeda')
def filter_formatar_moeda(valor):
    """Filtro para formatar valores monetários"""
    return formatar_moeda(valor)


@relatorios_bp.app_template_filter('formatar_percentual')
def filter_formatar_percentual(valor):
    """Filtro para formatar percentuais"""
    # O valor já vem em percentual, só precisa formatar
    return formatar_percentual(valor/100 if valor else 0, casas_decimais=2)


# ROTA PARA DOWNLOAD HTML GENÉRICO

@relatorios_bp.route('/download-html/<tipo_relatorio>')
def download_html_generico(tipo_relatorio):
    """Rota genérica para download de relatórios em HTML"""
    try:
        # Mapeia tipos de relatório para suas respectivas funções
        mapa_relatorios = {
            'balanco_orcamentario': 'balanco_orcamentario_receita',
            'inconsistencias': 'relatorio_inconsistencias',
            # Adicione outros relatórios aqui conforme necessário
        }
        
        if tipo_relatorio not in mapa_relatorios:
            return jsonify({"erro": "Tipo de relatório inválido"}), 400
        
        # Adiciona formato=html_download aos parâmetros
        args = request.args.to_dict()
        args['formato'] = 'html_download'
        
        # Redireciona para a rota específica com os parâmetros
        from flask import redirect, url_for
        
        if tipo_relatorio == 'balanco_orcamentario':
            return redirect(url_for('relatorios.balanco_orcamentario_receita', **args))
        elif tipo_relatorio == 'inconsistencias':
            return redirect(url_for('inconsistencias.relatorio_inconsistencias', **args))
        
    except Exception as e:
        return jsonify({"erro": str(e)}), 500


@relatorios_bp.route('/balanco-orcamentario-receita/lancamentos')
def buscar_lancamentos():
    """Mantém compatibilidade com a rota antiga - redireciona para a nova API"""
    return api_lancamentos()


@relatorios_bp.route('/api/relatorio-receita-fonte')
def api_relatorio_receita_fonte():
    """API para buscar dados do relatório por receita ou fonte"""
    try:
        # Obtém parâmetros
        tipo = request.args.get('tipo', 'receita')  # 'receita' ou 'fonte'
        ano = request.args.get('ano', type=int)
        mes = request.args.get('mes', type=int)
        coug = request.args.get('coug', '')
        filtro = request.args.get('filtro', None)
        
        # Valida tipo
        if tipo not in ['receita', 'fonte']:
            return jsonify({"erro": "Tipo inválido. Use 'receita' ou 'fonte'"}), 400
        
        # Conecta ao banco
        conn = ConexaoBanco.conectar_completo()
        
        # Gera o relatório
        resultado = gerar_relatorio_receita_fonte(
            conn=conn,
            tipo=tipo,
            ano=ano,
            mes=mes,
            coug=coug if coug else None,
            filtro_relatorio_key=filtro
        )
        
        conn.close()
        
        return jsonify(resultado)
        
    except Exception as e:
        traceback.print_exc()
        return jsonify({"erro": str(e)}), 500