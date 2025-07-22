import math
from flask import render_template, request, Blueprint
from app.relatorios.RREO_receita import BalancoOrcamentarioAnexo2  # CORRIGIDO: era RREO_balanco_orcamentario
from app.relatorios.RREO_balanco_intra import BalancoOrcamentarioIntraAnexo2  # NOVO IMPORT
from app.relatorios.calculo_superavit_deficit import CalculoSuperavitDeficit  # NOVO IMPORT
from app.modulos.conexao_hibrida import ConexaoBanco, adaptar_query
import pandas as pd
from datetime import datetime

# Se você já tem um blueprint definido, pode usar o mesmo
# Se não, este é um exemplo de como criar um
rreo_bp = Blueprint('rreo', __name__, url_prefix='/rreo')

def _get_periodo_padrao():
    """Busca no banco o último ano e bimestre com dados para usar como filtro padrão."""
    try:
        with ConexaoBanco() as conn:
            # Busca o último ano que tenha registros
            query_ano = "SELECT MAX(CAST(coexercicio AS INTEGER)) as ano FROM fato_saldos"
            df_ano = pd.read_sql_query(adaptar_query(query_ano), conn)
            # Se não houver dados, usa o ano atual como padrão
            ano_padrao = df_ano['ano'].iloc[0] if not df_ano.empty and not pd.isna(df_ano['ano'].iloc[0]) else datetime.now().year

            # Com o último ano, busca o último mês de registro
            query_mes = "SELECT MAX(CAST(inmes AS INTEGER)) as mes FROM fato_saldos WHERE CAST(coexercicio AS INTEGER) = ?"
            params_mes = [int(ano_padrao)]
            df_mes = pd.read_sql_query(adaptar_query(query_mes), conn, params=params_mes)
            # Se não houver, usa o mês 1 como padrão
            mes_padrao = df_mes['mes'].iloc[0] if not df_mes.empty and not pd.isna(df_mes['mes'].iloc[0]) else 1
            
            # Calcula o bimestre a partir do mês (Ex: Mês 6 / 2 = 3º Bi; Mês 7 / 2 = 3.5 -> 4º Bi)
            bimestre_padrao = math.ceil(mes_padrao / 2)
            
            return int(ano_padrao), int(bimestre_padrao)
    except Exception as e:
        print(f"Erro ao buscar período padrão: {e}")
        # Em caso de qualquer erro, retorna um valor padrão seguro
        return datetime.now().year, 1

@rreo_bp.route('/anexo2')
def balanco_orcamentario_anexo2():
    """ Rota para o Anexo 2 do RREO - Balanço Orçamentário (Receita e Despesa). """
    
    # Define o período padrão dinamicamente
    ano_padrao, bimestre_padrao = _get_periodo_padrao()
    
    # Pega os valores do filtro da URL ou usa o padrão
    ano_selecionado = request.args.get('ano', default=ano_padrao, type=int)
    bimestre_selecionado = request.args.get('bimestre', default=bimestre_padrao, type=int)

    # Busca os anos disponíveis para popular o dropdown do filtro
    with ConexaoBanco() as conn:
        df_anos = pd.read_sql_query(adaptar_query("SELECT DISTINCT CAST(coexercicio AS INTEGER) as ano FROM fato_saldos ORDER BY ano DESC"), conn)
        anos_disponiveis = df_anos['ano'].tolist() if not df_anos.empty else [datetime.now().year]

    # Gera os dados do relatório
    relatorio_builder = BalancoOrcamentarioAnexo2(ano=ano_selecionado, bimestre=bimestre_selecionado)
    dados_relatorio = relatorio_builder.gerar_relatorio()

    # NOVO: Calcula superávit/déficit separadamente
    calculo_superavit_deficit = CalculoSuperavitDeficit(ano=ano_selecionado, bimestre=bimestre_selecionado)
    dados_superavit_deficit = calculo_superavit_deficit.calcular()

    return render_template(
        'rreo/RREO_balanco_orcamentario.html',
        dados=dados_relatorio,
        superavit_deficit=dados_superavit_deficit,  # NOVO PARÂMETRO
        ano_selecionado=ano_selecionado,
        bimestre_selecionado=bimestre_selecionado,
        anos_disponiveis=anos_disponiveis
    )

@rreo_bp.route('/intra')
def balanco_orcamentario_intra():
    """ Rota para o RREO - Balanço Orçamentário Intra-Orçamentário (Receitas e Despesas Intra). """
    
    # Define o período padrão dinamicamente
    ano_padrao, bimestre_padrao = _get_periodo_padrao()
    
    # Pega os valores do filtro da URL ou usa o padrão
    ano_selecionado = request.args.get('ano', default=ano_padrao, type=int)
    bimestre_selecionado = request.args.get('bimestre', default=bimestre_padrao, type=int)

    # Busca os anos disponíveis para popular o dropdown do filtro
    with ConexaoBanco() as conn:
        df_anos = pd.read_sql_query(adaptar_query("SELECT DISTINCT CAST(coexercicio AS INTEGER) as ano FROM fato_saldos ORDER BY ano DESC"), conn)
        anos_disponiveis = df_anos['ano'].tolist() if not df_anos.empty else [datetime.now().year]

    # Gera os dados do relatório intra-orçamentário
    relatorio_builder = BalancoOrcamentarioIntraAnexo2(ano=ano_selecionado, bimestre=bimestre_selecionado)
    dados_relatorio = relatorio_builder.gerar_relatorio()

    return render_template(
        'rreo/RREO_balanco_intra.html',
        dados=dados_relatorio,
        ano_selecionado=ano_selecionado,
        bimestre_selecionado=bimestre_selecionado,
        anos_disponiveis=anos_disponiveis
    )