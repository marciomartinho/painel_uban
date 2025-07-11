# app/modulos/regras_contabeis_receita.py
"""Regras para classificação de contas contábeis da receita"""

import os
import sqlite3

# Definição das faixas de contas contábeis
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
        'filtro_sql': "COCONTACONTABIL BETWEEN '521110000' AND '521129999'",
        'calcular_como': 'PREVISAO_INICIAL - DEDUCOES_PREVISAO_INICIAL'
    },
    'PREVISAO_ATUALIZADA': {
        'descricao': 'Previsão Atualizada',
        'conta_inicio': '521200000',
        'conta_fim': '521299999',
        'filtro_sql': "COCONTACONTABIL BETWEEN '521200000' AND '521299999'"
    },
    'PREVISAO_ATUALIZADA_LIQUIDA': {
        'descricao': 'Previsão Atualizada Líquida',
        'conta_inicio': '521110000',
        'conta_fim': '521299999',
        'filtro_sql': "COCONTACONTABIL BETWEEN '521110000' AND '521299999'",
        'calcular_como': 'PREVISAO_ATUALIZADA'
    },
    'RECEITA_BRUTA': {
        'descricao': 'Receita Bruta',
        'conta_inicio': '621200000',
        'conta_fim': '621200000',
        'filtro_sql': "COCONTACONTABIL = '621200000'"
    },
    'DEDUCOES_RECEITA_BRUTA': {
        'descricao': 'Deduções da Receita Bruta',
        'conta_inicio': '621300000',
        'conta_fim': '621399999',
        'filtro_sql': "COCONTACONTABIL BETWEEN '621300000' AND '621399999'"
    },
    'RECEITA_LIQUIDA': {
        'descricao': 'Receita Líquida',
        'conta_inicio': '621200000',
        'conta_fim': '621399999',
        'filtro_sql': "COCONTACONTABIL BETWEEN '621200000' AND '621399999'",
        'calcular_como': 'RECEITA_BRUTA - DEDUCOES_RECEITA_BRUTA'
    }
}

def get_filtro_conta(tipo_conta):
    """Retorna o filtro SQL para um tipo específico de conta"""
    if tipo_conta in REGRAS_CONTAS:
        return REGRAS_CONTAS[tipo_conta]['filtro_sql']
    return None

def get_query_valor_por_tipo(tipo_conta, filtro_adicional=""):
    """Retorna query completa para calcular valor de um tipo de conta"""
    filtro = get_filtro_conta(tipo_conta)
    if not filtro:
        return None
    
    # Para tipos que são calculados (líquidos)
    if 'calcular_como' in REGRAS_CONTAS[tipo_conta]:
        if tipo_conta == 'PREVISAO_INICIAL_LIQUIDA':
            return f"""
            SELECT 
                SUM(CASE WHEN {get_filtro_conta('PREVISAO_INICIAL')} THEN VALANCAMENTO ELSE 0 END) -
                SUM(CASE WHEN {get_filtro_conta('DEDUCOES_PREVISAO_INICIAL')} THEN VALANCAMENTO ELSE 0 END) as valor
            FROM lancamentos
            WHERE {filtro} {' AND ' + filtro_adicional if filtro_adicional else ''}
            """
        elif tipo_conta == 'RECEITA_LIQUIDA':
            return f"""
            SELECT 
                SUM(CASE WHEN {get_filtro_conta('RECEITA_BRUTA')} THEN VALANCAMENTO ELSE 0 END) -
                SUM(CASE WHEN {get_filtro_conta('DEDUCOES_RECEITA_BRUTA')} THEN VALANCAMENTO ELSE 0 END) as valor
            FROM lancamentos
            WHERE {filtro} {' AND ' + filtro_adicional if filtro_adicional else ''}
            """
    
    # Para tipos simples (soma direta)
    return f"""
    SELECT SUM(VALANCAMENTO) as valor
    FROM lancamentos
    WHERE {filtro} {' AND ' + filtro_adicional if filtro_adicional else ''}
    """

def get_query_resumo_completo(filtro_periodo=""):
    """Retorna query para calcular todos os valores de uma vez"""
    return f"""
    SELECT 
        -- Previsões
        SUM(CASE WHEN {get_filtro_conta('PREVISAO_INICIAL')} THEN VALANCAMENTO ELSE 0 END) as previsao_inicial,
        SUM(CASE WHEN {get_filtro_conta('DEDUCOES_PREVISAO_INICIAL')} THEN VALANCAMENTO ELSE 0 END) as deducoes_previsao_inicial,
        SUM(CASE WHEN {get_filtro_conta('PREVISAO_INICIAL_LIQUIDA')} THEN VALANCAMENTO ELSE 0 END) as previsao_inicial_liquida_total,
        
        -- Previsão Atualizada
        SUM(CASE WHEN {get_filtro_conta('PREVISAO_ATUALIZADA')} THEN VALANCAMENTO ELSE 0 END) as previsao_atualizada,
        
        -- Receitas
        SUM(CASE WHEN {get_filtro_conta('RECEITA_BRUTA')} THEN VALANCAMENTO ELSE 0 END) as receita_bruta,
        SUM(CASE WHEN {get_filtro_conta('DEDUCOES_RECEITA_BRUTA')} THEN VALANCAMENTO ELSE 0 END) as deducoes_receita_bruta,
        
        -- Calculados
        SUM(CASE WHEN {get_filtro_conta('PREVISAO_INICIAL')} THEN VALANCAMENTO ELSE 0 END) -
        SUM(CASE WHEN {get_filtro_conta('DEDUCOES_PREVISAO_INICIAL')} THEN VALANCAMENTO ELSE 0 END) as previsao_inicial_liquida,
        
        SUM(CASE WHEN {get_filtro_conta('RECEITA_BRUTA')} THEN VALANCAMENTO ELSE 0 END) -
        SUM(CASE WHEN {get_filtro_conta('DEDUCOES_RECEITA_BRUTA')} THEN VALANCAMENTO ELSE 0 END) as receita_liquida
        
    FROM lancamentos
    {' WHERE ' + filtro_periodo if filtro_periodo else ''}
    """

def verificar_conta_tipo(conta_contabil):
    """Verifica a qual tipo pertence uma conta contábil específica"""
    conta = str(conta_contabil)
    tipos = []
    
    for tipo, regra in REGRAS_CONTAS.items():
        if conta >= regra['conta_inicio'] and conta <= regra['conta_fim']:
            tipos.append(tipo)
    
    return tipos

# TESTE
if __name__ == "__main__":
    print("=== TESTE DO MÓDULO REGRAS CONTÁBEIS ===")
    print()
    
    # Mostra todas as regras
    print("📋 Regras de Classificação de Contas:")
    for tipo, regra in REGRAS_CONTAS.items():
        print(f"\n{tipo}:")
        print(f"  - Descrição: {regra['descricao']}")
        print(f"  - Faixa: {regra['conta_inicio']} até {regra['conta_fim']}")
        print(f"  - Filtro SQL: {regra['filtro_sql']}")
        if 'calcular_como' in regra:
            print(f"  - Cálculo: {regra['calcular_como']}")
    
    # Testa verificação de contas
    print("\n\n🔍 Teste de Classificação de Contas:")
    contas_teste = ['521110001', '521125000', '521250000', '621200000', '621350000']
    for conta in contas_teste:
        tipos = verificar_conta_tipo(conta)
        print(f"  Conta {conta}: {', '.join(tipos) if tipos else 'Não classificada'}")
    
    # Mostra exemplo de query
    print("\n\n📝 Exemplo de Query Gerada:")
    print("Para RECEITA_LIQUIDA com filtro de período:")
    from periodo import get_filtro_acumulado
    query = get_query_valor_por_tipo('RECEITA_LIQUIDA', get_filtro_acumulado())
    print(query)
    
    # Testa com dados reais
    print("\n\n📊 Teste com Dados Reais:")
    import sqlite3
    caminho_db = os.path.join(os.path.dirname(__file__), '../../banco_lancamento_receita.db')
    
    try:
        conn = sqlite3.connect(caminho_db)
        
        # Busca resumo completo
        query_resumo = get_query_resumo_completo(get_filtro_acumulado())
        resultado = conn.execute(query_resumo).fetchone()
        
        if resultado:
            print(f"  Previsão Inicial: R$ {resultado[0]:,.2f}")
            print(f"  Deduções Prev. Inicial: R$ {resultado[1]:,.2f}")
            print(f"  Previsão Inicial Líquida: R$ {resultado[6]:,.2f}")
            print(f"  Previsão Atualizada: R$ {resultado[3]:,.2f}")
            print(f"  Receita Bruta: R$ {resultado[4]:,.2f}")
            print(f"  Deduções Receita: R$ {resultado[5]:,.2f}")
            print(f"  Receita Líquida: R$ {resultado[7]:,.2f}")
            
        conn.close()
    except Exception as e:
        print(f"  Erro ao acessar banco: {e}")