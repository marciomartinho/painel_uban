# app/modulos/regras_contabeis_receita.py
"""Regras para classificação de contas contábeis e filtros de relatórios"""

# <<< CORREÇÃO: Todas as regras de filtro SQL agora usam 'cocontacontabil' em minúsculas >>>
REGRAS_CONTAS = {
    'PREVISAO_INICIAL': {
        'descricao': 'Previsão Inicial',
        'filtro_sql': "cocontacontabil BETWEEN '521110000' AND '521119999'"
    },
    'DEDUCOES_PREVISAO_INICIAL': {
        'descricao': 'Deduções da Previsão Inicial',
        'filtro_sql': "cocontacontabil BETWEEN '521120000' AND '521129999'"
    },
    'PREVISAO_INICIAL_LIQUIDA': {
        'descricao': 'Previsão Inicial Líquida',
        'filtro_sql': "cocontacontabil BETWEEN '521110000' AND '521129999'"
    },
    'PREVISAO_ATUALIZADA': {
        'descricao': 'Previsão Atualizada',
        'filtro_sql': "cocontacontabil BETWEEN '521100000' AND '521299999'"
    },
    'PREVISAO_ATUALIZADA_LIQUIDA': {
        'descricao': 'Previsão Atualizada Líquida',
        'filtro_sql': "cocontacontabil BETWEEN '521110000' AND '521299999'"
    },
    'RECEITA_BRUTA': {
        'descricao': 'Receita Bruta',
        'filtro_sql': "cocontacontabil = '621200000'"
    },
    'DEDUCOES_RECEITA_BRUTA': {
        'descricao': 'Dedução da Receita Bruta',
        'filtro_sql': "cocontacontabil BETWEEN '621300000' AND '621399999'"
    },
    'RECEITA_LIQUIDA': {
        'descricao': 'Receita Líquida',
        'filtro_sql': "cocontacontabil BETWEEN '621200000' AND '621399999'"
    }
}

# <<< CORREÇÃO: campo_filtro também em minúsculas para garantir consistência >>>
FILTROS_RELATORIO_ESPECIAIS = {
    'tributarias': {
        'descricao': 'Receitas Tributárias',
        'campo_filtro': 'cofontereceita',
        'valores': ['11', '71']
    },
    'contribuicoes': {
        'descricao': 'Receitas de Contribuições',
        'campo_filtro': 'cofontereceita',
        'valores': ['12', '72']
    },
    'patrimonial': {
        'descricao': 'Receita Patrimonial',
        'campo_filtro': 'cofontereceita',
        'valores': ['13', '73']
    },
    'agropecuaria': {
        'descricao': 'Receita Agropecuária',
        'campo_filtro': 'cofontereceita',
        'valores': ['14', '74']
    },
    'industrial': {
        'descricao': 'Receita Industrial',
        'campo_filtro': 'cofontereceita',
        'valores': ['15', '75']
    },
    'servicos': {
        'descricao': 'Receita de Serviços',
        'campo_filtro': 'cofontereceita',
        'valores': ['16', '76']
    },
    'transf_correntes': {
        'descricao': 'Transferências Correntes',
        'campo_filtro': 'cofontereceita',
        'valores': ['17', '77']
    },
    'outras_correntes': {
        'descricao': 'Outras Receitas Correntes',
        'campo_filtro': 'cofontereceita',
        'valores': ['19', '79']
    },
    'op_credito': {
        'descricao': 'Operações de Crédito',
        'campo_filtro': 'cofontereceita',
        'valores': ['21']
    },
    'alienacao_bens': {
        'descricao': 'Alienação de Bens',
        'campo_filtro': 'cofontereceita',
        'valores': ['22']
    },
    'amortizacao': {
        'descricao': 'Amortização de Empréstimos',
        'campo_filtro': 'cofontereceita',
        'valores': ['23']
    },
    'transf_capital': {
        'descricao': 'Transferências de Capital',
        'campo_filtro': 'cofontereceita',
        'valores': ['24']
    }
}


def get_filtro_conta(tipo_conta):
    """Retorna o filtro SQL para um tipo específico de conta"""
    return REGRAS_CONTAS.get(tipo_conta, {}).get('filtro_sql', "1=0")