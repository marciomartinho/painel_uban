# app/modulos/regras_contabeis_receita.py
"""Regras para classificação de contas contábeis e filtros de relatórios"""

# --- CORREÇÃO: Todas as regras de filtro SQL agora usam 'cocontacontabil' em minúsculas ---
REGRAS_CONTAS = {
    'PREVISAO_INICIAL': {
        'descricao': 'Previsão Inicial',
        'conta_inicio': '521110000',
        'conta_fim': '521119999',
        'filtro_sql': "cocontacontabil BETWEEN '521110000' AND '521119999'"
    },
    'DEDUCOES_PREVISAO_INICIAL': {
        'descricao': 'Deduções da Previsão Inicial',
        'conta_inicio': '521120000',
        'conta_fim': '521129999',
        'filtro_sql': "cocontacontabil BETWEEN '521120000' AND '521129999'"
    },
    'PREVISAO_INICIAL_LIQUIDA': {
        'descricao': 'Previsão Inicial Líquida',
        'conta_inicio': '521110000',
        'conta_fim': '521129999',
        'filtro_sql': "cocontacontabil BETWEEN '521110000' AND '521129999'"
    },
    'PREVISAO_ATUALIZADA': {
        'descricao': 'Previsão Atualizada',
        'conta_inicio': '521100000',
        'conta_fim': '521299999',
        'filtro_sql': "cocontacontabil BETWEEN '521100000' AND '521299999'"
    },
    'PREVISAO_ATUALIZADA_LIQUIDA': {
        'descricao': 'Previsão Atualizada Líquida',
        'conta_inicio': '521110000',
        'conta_fim': '521299999',
        'filtro_sql': "cocontacontabil BETWEEN '521110000' AND '521299999'"
    },
    'RECEITA_BRUTA': {
        'descricao': 'Receita Bruta',
        'conta_inicio': '621200000',
        'conta_fim': '621200000',
        'filtro_sql': "cocontacontabil = '621200000'"
    },
    'DEDUCOES_RECEITA_BRUTA': {
        'descricao': 'Dedução da Receita Bruta',
        'conta_inicio': '621300000',
        'conta_fim': '621399999',
        'filtro_sql': "cocontacontabil BETWEEN '621300000' AND '621399999'"
    },
    'RECEITA_LIQUIDA': {
        'descricao': 'Receita Líquida',
        'conta_inicio': '621200000',
        'conta_fim': '621399999',
        'filtro_sql': "cocontacontabil BETWEEN '621200000' AND '621399999'"
    }
}

FILTROS_RELATORIO_ESPECIAIS = {
    'tributarias': {
        'descricao': 'Receitas Tributárias',
        'campo_filtro': 'COFONTERECEITA', # Este será convertido para minúsculas na query
        'valores': ['11', '71']
    },
    'contribuicoes': {
        'descricao': 'Receitas de Contribuições',
        'campo_filtro': 'COFONTERECEITA',
        'valores': ['12', '72']
    },
    # ... (o resto do dicionário não precisa de alteração)
    'patrimonial': {
        'descricao': 'Receita Patrimonial',
        'campo_filtro': 'COFONTERECEITA',
        'valores': ['13', '73']
    },
    'agropecuaria': {
        'descricao': 'Receita Agropecuária',
        'campo_filtro': 'COFONTERECEITA',
        'valores': ['14', '74']
    },
    'industrial': {
        'descricao': 'Receita Industrial',
        'campo_filtro': 'COFONTERECEITA',
        'valores': ['15', '75']
    },
    'servicos': {
        'descricao': 'Receita de Serviços',
        'campo_filtro': 'COFONTERECEITA',
        'valores': ['16', '76']
    },
    'transf_correntes': {
        'descricao': 'Transferências Correntes',
        'campo_filtro': 'COFONTERECEITA',
        'valores': ['17', '77']
    },
    'outras_correntes': {
        'descricao': 'Outras Receitas Correntes',
        'campo_filtro': 'COFONTERECEITA',
        'valores': ['19', '79']
    },
    'op_credito': {
        'descricao': 'Operações de Crédito',
        'campo_filtro': 'COFONTERECEITA',
        'valores': ['21']
    },
    'alienacao_bens': {
        'descricao': 'Alienação de Bens',
        'campo_filtro': 'COFONTERECEITA',
        'valores': ['22']
    },
    'amortizacao': {
        'descricao': 'Amortização de Empréstimos',
        'campo_filtro': 'COFONTERECEITA',
        'valores': ['23']
    },
    'transf_capital': {
        'descricao': 'Transferências de Capital',
        'campo_filtro': 'COFONTERECEITA',
        'valores': ['24']
    }
}


def get_filtro_conta(tipo_conta):
    """Retorna o filtro SQL para um tipo específico de conta"""
    if tipo_conta in REGRAS_CONTAS:
        return REGRAS_CONTAS[tipo_conta]['filtro_sql']
    return "1=0" # Retorna uma condição falsa se o tipo não for encontrado

