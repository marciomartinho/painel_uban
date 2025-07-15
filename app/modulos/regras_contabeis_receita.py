# app/modulos/regras_contabeis_receita.py
"""Regras para classificação de contas contábeis e filtros de relatórios"""

import os
import sqlite3

# ==============================================================================
# REGRAS DINÂMICAS PARA FILTROS ESPECIAIS DE RELATÓRIO
# ==============================================================================
# Estrutura para definir filtros personalizados que podem ser acionados via URL.
# Para adicionar um novo botão de filtro, basta adicionar uma nova entrada neste dicionário.
FILTROS_RELATORIO_ESPECIAIS = {
    'tributarias': {
        'descricao': 'Receitas Tributárias',
        'campo_filtro': 'COFONTERECEITA',
        'valores': ['11', '71'] # Impostos, Taxas e Contribuições de Melhoria
    },
    'contribuicoes': {
        'descricao': 'Receitas de Contribuições',
        'campo_filtro': 'COFONTERECEITA',
        'valores': ['12', '72']
    },
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


# ==============================================================================
# Definição das faixas de contas contábeis
# ==============================================================================
REGRAS_CONTAS = {
    'PREVISAO_INICIAL': {
        'descricao': 'Previsão Inicial',
        'conta_inicio': '521110000',
        'conta_fim': '521119999',
        'filtro_sql': "COCONTACONTABIL BETWEEN '521110000' AND '521119999'"
    },
    'DEDUCOES_PREVISAO_INICIAL': {
        'descricao': 'Deduções da Previsão Inicial',
        'conta_inicio': '521120000',
        'conta_fim': '521129999',
        'filtro_sql': "COCONTACONTABIL BETWEEN '521120000' AND '521129999'"
    },
    'PREVISAO_INICIAL_LIQUIDA': {
        'descricao': 'Previsão Inicial Líquida',
        'conta_inicio': '521110000',
        'conta_fim': '521129999',
        'filtro_sql': "COCONTACONTABIL BETWEEN '521110000' AND '521129999'"
    },
    'PREVISAO_ATUALIZADA': {
        'descricao': 'Previsão Atualizada',
        'conta_inicio': '521100000',
        'conta_fim': '521299999',
        'filtro_sql': "COCONTACONTABIL BETWEEN '521100000' AND '521299999'"
    },
    'PREVISAO_ATUALIZADA_LIQUIDA': {
        'descricao': 'Previsão Atualizada Líquida',
        'conta_inicio': '521110000',
        'conta_fim': '521299999',
        'filtro_sql': "COCONTACONTABIL BETWEEN '521110000' AND '521299999'"
    },
    'RECEITA_BRUTA': {
        'descricao': 'Receita Bruta',
        'conta_inicio': '621200000',
        'conta_fim': '621200000',
        'filtro_sql': "COCONTACONTABIL = '621200000'"
    },
    'DEDUCOES_RECEITA_BRUTA': {
        'descricao': 'Dedução da Receita Bruta',
        'conta_inicio': '621300000',
        'conta_fim': '621399999',
        'filtro_sql': "COCONTACONTABIL BETWEEN '621300000' AND '621399999'"
    },
    'RECEITA_LIQUIDA': {
        'descricao': 'Receita Líquida',
        'conta_inicio': '621200000',
        'conta_fim': '621399999',
        'filtro_sql': "COCONTACONTABIL BETWEEN '621200000' AND '621399999'"
    }
}

# Definição da hierarquia de agregação
NIVEIS_AGREGACAO = [
    {
        'nivel': 1,
        'campo': 'CATEGORIARECEITA',
        'tabela_dimensao': 'categorias',
        'campo_codigo': 'COCATEGORIARECEITA',
        'campo_nome': 'NOCATEGORIARECEITA',
        'descricao': 'Categoria da Receita'
    },
    {
        'nivel': 2,
        'campo': 'COFONTERECEITA',
        'tabela_dimensao': 'origens',
        'campo_codigo': 'COFONTERECEITA',
        'campo_nome': 'NOFONTERECEITA',
        'descricao': 'Fonte da Receita'
    },
    {
        'nivel': 3,
        'campo': 'COSUBFONTERECEITA',
        'tabela_dimensao': 'especies',
        'campo_codigo': 'COSUBFONTERECEITA',
        'campo_nome': 'NOSUBFONTERECEITA',
        'descricao': 'Subfonte da Receita'
    },
    {
        'nivel': 4,
        'campo': 'CORUBRICA',
        'tabela_dimensao': 'especificacoes',
        'campo_codigo': 'CORUBRICA',
        'campo_nome': 'NORUBRICA',
        'descricao': 'Rubrica'
    },
    {
        'nivel': 5,
        'campo': 'COALINEA',
        'tabela_dimensao': 'alineas',
        'campo_codigo': 'COALINEA',
        'campo_nome': 'NOALINEA',
        'descricao': 'Alínea'
    }
]

def get_filtro_conta(tipo_conta):
    """Retorna o filtro SQL para um tipo específico de conta"""
    if tipo_conta in REGRAS_CONTAS:
        return REGRAS_CONTAS[tipo_conta]['filtro_sql']
    return None

def get_query_valor_por_tipo(tipo_conta, filtro_adicional=""):
    """Retorna query para calcular valor de um tipo de conta específico"""
    filtro = get_filtro_conta(tipo_conta)
    if not filtro:
        return None
    
    return f"""
    SELECT 
        COALESCE(SUM(VALANCAMENTO), 0) as valor
    FROM lancamentos
    WHERE {filtro}
    {' AND ' + filtro_adicional if filtro_adicional else ''}
    """

def get_query_resumo_completo(filtro_periodo=""):
    """Retorna query para calcular todos os valores de uma vez"""
    return f"""
    SELECT 
        -- Previsões
        COALESCE(SUM(CASE WHEN {get_filtro_conta('PREVISAO_INICIAL')} THEN VALANCAMENTO ELSE 0 END), 0) as previsao_inicial,
        COALESCE(SUM(CASE WHEN {get_filtro_conta('DEDUCOES_PREVISAO_INICIAL')} THEN VALANCAMENTO ELSE 0 END), 0) as deducoes_previsao_inicial,
        COALESCE(SUM(CASE WHEN {get_filtro_conta('PREVISAO_INICIAL_LIQUIDA')} THEN VALANCAMENTO ELSE 0 END), 0) as previsao_inicial_liquida,
        
        -- Previsão Atualizada
        COALESCE(SUM(CASE WHEN {get_filtro_conta('PREVISAO_ATUALIZADA')} THEN VALANCAMENTO ELSE 0 END), 0) as previsao_atualizada,
        COALESCE(SUM(CASE WHEN {get_filtro_conta('PREVISAO_ATUALIZADA_LIQUIDA')} THEN VALANCAMENTO ELSE 0 END), 0) as previsao_atualizada_liquida,
        
        -- Receitas
        COALESCE(SUM(CASE WHEN {get_filtro_conta('RECEITA_BRUTA')} THEN VALANCAMENTO ELSE 0 END), 0) as receita_bruta,
        COALESCE(SUM(CASE WHEN {get_filtro_conta('DEDUCOES_RECEITA_BRUTA')} THEN VALANCAMENTO ELSE 0 END), 0) as deducoes_receita_bruta,
        COALESCE(SUM(CASE WHEN {get_filtro_conta('RECEITA_LIQUIDA')} THEN VALANCAMENTO ELSE 0 END), 0) as receita_liquida
        
    FROM lancamentos
    {' WHERE ' + filtro_periodo if filtro_periodo else ''}
    """

def get_query_demonstrativo_receita(nivel_detalhamento=1, filtro_periodo=""):
    """
    Gera query para o demonstrativo da receita com agregação hierárquica
    
    Args:
        nivel_detalhamento: de 1 a 5, define até qual nível detalhar
        filtro_periodo: filtro adicional para período
    
    Returns:
        String com a query SQL
    """
    
    # Monta os campos de seleção e joins com base no nível de detalhamento
    campos_select = []
    joins = []
    group_by = []
    order_by = []
    
    for nivel in NIVEIS_AGREGACAO:
        if nivel['nivel'] <= nivel_detalhamento:
            # Adiciona campo de código e nome
            campos_select.append(f"l.{nivel['campo']}")
            campos_select.append(f"d{nivel['nivel']}.{nivel['campo_nome']} as {nivel['campo_nome']}")
            
            # Adiciona join
            joins.append(f"""
            LEFT JOIN {nivel['tabela_dimensao']} d{nivel['nivel']} 
                ON l.{nivel['campo']} = d{nivel['nivel']}.{nivel['campo_codigo']}""")
            
            # Adiciona ao group by e order by
            group_by.append(f"l.{nivel['campo']}")
            group_by.append(f"d{nivel['nivel']}.{nivel['campo_nome']}")
            order_by.append(f"l.{nivel['campo']}")
    
    # Monta a query completa
    query = f"""
    SELECT 
        {', '.join(campos_select)},
        
        -- Valores calculados por tipo de conta
        COALESCE(SUM(CASE WHEN {get_filtro_conta('PREVISAO_INICIAL')} THEN VALANCAMENTO ELSE 0 END), 0) as previsao_inicial,
        COALESCE(SUM(CASE WHEN {get_filtro_conta('DEDUCOES_PREVISAO_INICIAL')} THEN VALANCAMENTO ELSE 0 END), 0) as deducoes_previsao_inicial,
        COALESCE(SUM(CASE WHEN {get_filtro_conta('PREVISAO_INICIAL_LIQUIDA')} THEN VALANCAMENTO ELSE 0 END), 0) as previsao_inicial_liquida,
        COALESCE(SUM(CASE WHEN {get_filtro_conta('PREVISAO_ATUALIZADA')} THEN VALANCAMENTO ELSE 0 END), 0) as previsao_atualizada,
        COALESCE(SUM(CASE WHEN {get_filtro_conta('PREVISAO_ATUALIZADA_LIQUIDA')} THEN VALANCAMENTO ELSE 0 END), 0) as previsao_atualizada_liquida,
        COALESCE(SUM(CASE WHEN {get_filtro_conta('RECEITA_BRUTA')} THEN VALANCAMENTO ELSE 0 END), 0) as receita_bruta,
        COALESCE(SUM(CASE WHEN {get_filtro_conta('DEDUCOES_RECEITA_BRUTA')} THEN VALANCamento ELSE 0 END), 0) as deducoes_receita_bruta,
        COALESCE(SUM(CASE WHEN {get_filtro_conta('RECEITA_LIQUIDA')} THEN VALANCAMENTO ELSE 0 END), 0) as receita_liquida
        
    FROM lancamentos l
    {''.join(joins)}
    {' WHERE ' + filtro_periodo if filtro_periodo else ''}
    GROUP BY {', '.join(group_by)}
    ORDER BY {', '.join(order_by)}
    """
    
    return query

def get_query_totalizador_nivel(nivel, filtro_periodo=""):
    """
    Gera query para totalizar valores por um nível específico
    """
    if nivel < 1 or nivel > 5:
        raise ValueError("Nível deve estar entre 1 e 5")
    
    nivel_info = NIVEIS_AGREGACAO[nivel - 1]
    
    query = f"""
    SELECT 
        l.{nivel_info['campo']},
        d.{nivel_info['campo_nome']},
        
        -- Totais por tipo
        COALESCE(SUM(CASE WHEN {get_filtro_conta('PREVISAO_INICIAL')} THEN VALANCAMENTO ELSE 0 END), 0) as previsao_inicial,
        COALESCE(SUM(CASE WHEN {get_filtro_conta('DEDUCOES_PREVISAO_INICIAL')} THEN VALANCAMENTO ELSE 0 END), 0) as deducoes_previsao_inicial,
        COALESCE(SUM(CASE WHEN {get_filtro_conta('PREVISAO_INICIAL_LIQUIDA')} THEN VALANCAMENTO ELSE 0 END), 0) as previsao_inicial_liquida,
        COALESCE(SUM(CASE WHEN {get_filtro_conta('PREVISAO_ATUALIZADA')} THEN VALANCAMENTO ELSE 0 END), 0) as previsao_atualizada,
        COALESCE(SUM(CASE WHEN {get_filtro_conta('PREVISAO_ATUALIZADA_LIQUIDA')} THEN VALANCAMENTO ELSE 0 END), 0) as previsao_atualizada_liquida,
        COALESCE(SUM(CASE WHEN {get_filtro_conta('RECEITA_BRUTA')} THEN VALANCAMENTO ELSE 0 END), 0) as receita_bruta,
        COALESCE(SUM(CASE WHEN {get_filtro_conta('DEDUCOES_RECEITA_BRUTA')} THEN VALANCAMENTO ELSE 0 END), 0) as deducoes_receita_bruta,
        COALESCE(SUM(CASE WHEN {get_filtro_conta('RECEITA_LIQUIDA')} THEN VALANCAMENTO ELSE 0 END), 0) as receita_liquida
        
    FROM lancamentos l
    LEFT JOIN {nivel_info['tabela_dimensao']} d 
        ON l.{nivel_info['campo']} = d.{nivel_info['campo_codigo']}
    {' WHERE ' + filtro_periodo if filtro_periodo else ''}
    GROUP BY l.{nivel_info['campo']}, d.{nivel_info['campo_nome']}
    ORDER BY l.{nivel_info['campo']}
    """
    
    return query

def verificar_conta_tipo(conta_contabil):
    """Verifica a qual tipo pertence uma conta contábil específica"""
    conta = str(conta_contabil)
    tipos = []
    
    for tipo, regra in REGRAS_CONTAS.items():
        if conta >= regra['conta_inicio'] and conta <= regra['conta_fim']:
            tipos.append(tipo)
    
    return tipos

def get_descricao_nivel(nivel):
    """Retorna a descrição de um nível de agregação"""
    if 1 <= nivel <= len(NIVEIS_AGREGACAO):
        return NIVEIS_AGREGACAO[nivel - 1]['descricao']
    return None

# TESTE
if __name__ == "__main__":
    print("=== TESTE DO MÓDULO REGRAS CONTÁBEIS (VERSÃO CORRIGIDA) ===")
    print()
    
    # Mostra todas as regras
    print("📋 Regras de Classificação de Contas:")
    for tipo, regra in REGRAS_CONTAS.items():
        print(f"\n{tipo}:")
        print(f"  - Descrição: {regra['descricao']}")
        print(f"  - Faixa: {regra['conta_inicio']} até {regra['conta_fim']}")
        print(f"  - Filtro SQL: {regra['filtro_sql']}")
    
    # Mostra níveis de agregação
    print("\n\n📊 Níveis de Agregação:")
    for nivel in NIVEIS_AGREGACAO:
        print(f"\nNível {nivel['nivel']} - {nivel['descricao']}:")
        print(f"  - Campo: {nivel['campo']}")
        print(f"  - Tabela dimensão: {nivel['tabela_dimensao']}")
        print(f"  - Campo código: {nivel['campo_codigo']}")
        print(f"  - Campo nome: {nivel['campo_nome']}")
    
    # Testa verificação de contas
    print("\n\n🔍 Teste de Classificação de Contas:")
    contas_teste = ['521110001', '521125000', '521250000', '621200000', '621350000']
    for conta in contas_teste:
        tipos = verificar_conta_tipo(conta)
        print(f"  Conta {conta}: {', '.join(tipos) if tipos else 'Não classificada'}")
    
    # Mostra exemplo de query do demonstrativo
    print("\n\n📝 Exemplo de Query do Demonstrativo (Nível 2):")
    from periodo import get_filtro_acumulado
    query = get_query_demonstrativo_receita(nivel_detalhamento=2, filtro_periodo=get_filtro_acumulado())
    print(query)
    
    # Testa com dados reais
    print("\n\n📊 Teste com Dados Reais (Resumo Geral):")
    caminho_db = os.path.join(os.path.dirname(__file__), '../../banco_lancamento_receita.db')
    
    try:
        conn = sqlite3.connect(caminho_db)
        
        # Busca resumo completo
        query_resumo = get_query_resumo_completo(get_filtro_acumulado())
        resultado = conn.execute(query_resumo).fetchone()
        
        if resultado:
            print(f"  Previsão Inicial: R$ {resultado[0]:,.2f}")
            print(f"  Deduções Prev. Inicial: R$ {resultado[1]:,.2f}")
            print(f"  Previsão Inicial Líquida: R$ {resultado[2]:,.2f}")
            print(f"  Previsão Atualizada: R$ {resultado[3]:,.2f}")
            print(f"  Previsão Atualizada Líquida: R$ {resultado[4]:,.2f}")
            print(f"  Receita Bruta: R$ {resultado[5]:,.2f}")
            print(f"  Deduções Receita Bruta: R$ {resultado[6]:,.2f}")
            print(f"  Receita Líquida: R$ {resultado[7]:,.2f}")
        
        # Testa query por categoria (nível 1)
        print("\n\n📊 Teste por Categoria (Nível 1):")
        query_categoria = get_query_totalizador_nivel(1, get_filtro_acumulado())
        cursor = conn.execute(query_categoria)
        
        for row in cursor.fetchmany(5):  # Mostra apenas as 5 primeiras
            print(f"\n  Categoria {row[0]} - {row[1]}:")
            print(f"    Previsão Inicial: R$ {row[2]:,.2f}")
            print(f"    Receita Bruta: R$ {row[7]:,.2f}")
            print(f"    Receita Líquida: R$ {row[9]:,.2f}")
            
        conn.close()
    except Exception as e:
        print(f"  Erro ao acessar banco: {e}")
