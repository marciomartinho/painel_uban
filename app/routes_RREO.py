# app/routes_RREO.py
"""
Rotas para o relatório Demonstrativo da Execução Orçamentária da Receita (Anexo 2).
"""

from flask import Blueprint, render_template, request
import traceback
from datetime import datetime

# Tente um import mais específico
try:
    from app.relatorios.RREO_balanco_orcamentario import BalancoOrcamentarioAnexo2
except ImportError:
    # Se falhar, tente import relativo
    from .relatorios.RREO_balanco_orcamentario import BalancoOrcamentarioAnexo2

# Cria o Blueprint para este relatório específico
anexo2_bp = Blueprint(
    'anexo2',
    __name__,
    url_prefix='/rreo'
)

@anexo2_bp.route('/anexo2')
def balanco_orcamentario_anexo2():
    """
    Renderiza a página do Demonstrativo da Execução Orçamentária da Receita.
    """
    try:
        ano_atual = datetime.now().year
        ano = request.args.get('ano', default=ano_atual, type=int)
        
        mes_atual = datetime.now().month
        bimestre_atual = (mes_atual + 1) // 2
        bimestre = request.args.get('bimestre', default=bimestre_atual, type=int)

        relatorio_builder = BalancoOrcamentarioAnexo2(ano=ano, bimestre=bimestre)
        dados_relatorio = relatorio_builder.gerar_relatorio()

        anos_disponiveis = range(ano_atual, ano_atual - 5, -1)
        
        return render_template(
            'rreo/RREO_balanco_orcamentario.html',  # Nome ajustado
            dados=dados_relatorio,
            ano_selecionado=ano,
            bimestre_selecionado=bimestre,
            anos_disponiveis=anos_disponiveis
        )
    except Exception as e:
        traceback.print_exc()
        return render_template('erro.html', mensagem=f"Erro ao gerar o relatório do Anexo 2: {e}")

@anexo2_bp.app_template_filter('formatar_moeda')
def formatar_moeda_filter(valor):
    """Filtro para formatar valores monetários"""
    if valor is None or valor == 0:
        return "-"
    
    # Formata o valor como moeda brasileira
    valor_formatado = f"{valor:,.2f}"
    # Substitui separadores para padrão brasileiro
    valor_formatado = valor_formatado.replace(',', '_').replace('.', ',').replace('_', '.')
    return f"R$ {valor_formatado}"