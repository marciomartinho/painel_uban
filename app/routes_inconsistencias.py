# app/routes_inconsistencias.py

from flask import Blueprint, render_template, request
# <<< MUDANÇA >>> Importa a nova função de análise
from app.relatorios.analise_inconsistencias import (
    analisar_fontes_superavit, 
    analisar_ugs_invalidas, 
    analisar_saldos_negativos,
    obter_exercicios_disponiveis
)
import datetime

inconsistencias_bp = Blueprint(
    'inconsistencias',
    __name__,
    template_folder='templates'
)

@inconsistencias_bp.route('/inconsistencias/relatorio')
def relatorio_inconsistencias():
    exercicios = obter_exercicios_disponiveis()
    exercicio_selecionado = request.args.get('exercicio', default=exercicios[0] if exercicios else 2025, type=int)
    data_geracao = datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S')

    # Executa todas as análises
    dados_fontes_superavit = analisar_fontes_superavit(exercicio_selecionado)
    dados_ugs_invalidas = analisar_ugs_invalidas(exercicio_selecionado)
    # <<< ADICIONADO >>> Chama a nova função de análise
    dados_saldos_negativos = analisar_saldos_negativos(exercicio_selecionado)

    return render_template(
        'relatorios_inconsistencias/relatorio_inconsistencias.html',
        dados_fontes_superavit=dados_fontes_superavit,
        dados_ugs_invalidas=dados_ugs_invalidas,
        # <<< ADICIONADO >>> Passa os novos dados para o template
        dados_saldos_negativos=dados_saldos_negativos,
        exercicios=exercicios,
        exercicio_selecionado=exercicio_selecionado,
        data_geracao=data_geracao
    )