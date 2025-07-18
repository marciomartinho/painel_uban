# app/routes_inconsistencias.py

from flask import Blueprint, render_template, request
# Importa as funções de análise do "cérebro"
from .relatorios.analise_inconsistencias import (
    analisar_fontes_superavit, 
    analisar_ugs_invalidas, 
    analisar_saldos_negativos,
    obter_exercicios_disponiveis
)
import datetime

inconsistencias_bp = Blueprint(
    'inconsistencias',
    __name__,
    # Aponta para a pasta correta onde o template está
    template_folder='templates/relatorios_inconsistencias'
)

# Define a URL
@inconsistencias_bp.route('/relatorio')
def relatorio_inconsistencias():
    # Pega os dados do "cérebro"
    exercicios = obter_exercicios_disponiveis()
    exercicio_selecionado = request.args.get('exercicio', default=exercicios[0] if exercicios else 2025, type=int)
    data_geracao = datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S')

    dados_fontes_superavit = analisar_fontes_superavit(exercicio_selecionado)
    dados_ugs_invalidas = analisar_ugs_invalidas(exercicio_selecionado)
    dados_saldos_negativos = analisar_saldos_negativos(exercicio_selecionado)

    # Entrega os dados para o "cardápio"
    return render_template(
        'relatorio_inconsistencias.html',
        dados_fontes_superavit=dados_fontes_superavit,
        dados_ugs_invalidas=dados_ugs_invalidas,
        dados_saldos_negativos=dados_saldos_negativos,
        exercicios=exercicios,
        exercicio_selecionado=exercicio_selecionado,
        data_geracao=data_geracao
    )