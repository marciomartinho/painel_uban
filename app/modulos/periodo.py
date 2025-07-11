# app/modulos/periodo.py
"""Gerenciamento do período de referência para relatórios"""

import sqlite3
import os
from datetime import datetime

def obter_periodo_referencia():
    """Retorna o mês e ano de referência baseado no último INMES disponível"""
    
    # Usa o caminho do banco que já deve estar definido em acesso_db.py
    caminho_db = os.path.join(os.path.dirname(__file__), '../../banco_lancamento_receita.db')
    conn = sqlite3.connect(caminho_db)
    
    # Busca o maior INMES
    query = """
    SELECT MAX(mes_ano) FROM (
        SELECT MAX(COEXERCICIO * 100 + INMES) as mes_ano FROM lancamentos
        UNION ALL
        SELECT MAX(COEXERCICIO * 100 + INMES) as mes_ano FROM fato_saldos
    )
    """
    
    resultado = conn.execute(query).fetchone()[0]
    conn.close()
    
    if resultado:
        ano = resultado // 100
        mes = resultado % 100
        
        meses_nomes = {
            1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril",
            5: "Maio", 6: "Junho", 7: "Julho", 8: "Agosto",
            9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"
        }
        
        return {
            'mes': mes,
            'ano': ano,
            'mes_nome': meses_nomes.get(mes, ""),
            'periodo_completo': f"{meses_nomes.get(mes, "")}/{ano}"
        }
    
    return None

def get_filtro_acumulado():
    """Retorna string de filtro SQL para período acumulado"""
    periodo = obter_periodo_referencia()
    if periodo:
        return f"COEXERCICIO = {periodo['ano']} AND INMES <= {periodo['mes']}"
    return "1=1"

def get_filtro_mes_atual():
    """Retorna string de filtro SQL apenas para o mês atual"""
    periodo = obter_periodo_referencia()
    if periodo:
        return f"COEXERCICIO = {periodo['ano']} AND INMES = {periodo['mes']}"
    return "1=1"

def get_filtro_acumulado_ano_anterior():
    """Retorna filtro para o mesmo período acumulado do ano anterior"""
    periodo = obter_periodo_referencia()
    if periodo:
        ano_anterior = periodo['ano'] - 1
        return f"COEXERCICIO = {ano_anterior} AND INMES <= {periodo['mes']}"
    return "1=1"

def get_filtro_mes_ano_anterior():
    """Retorna filtro para o mesmo mês do ano anterior"""
    periodo = obter_periodo_referencia()
    if periodo:
        ano_anterior = periodo['ano'] - 1
        return f"COEXERCICIO = {ano_anterior} AND INMES = {periodo['mes']}"
    return "1=1"

def get_filtro_comparativo():
    """Retorna filtro para buscar dados dos dois anos para comparação"""
    periodo = obter_periodo_referencia()
    if periodo:
        return f"""
        ((COEXERCICIO = {periodo['ano']} AND INMES <= {periodo['mes']}) 
        OR 
        (COEXERCICIO = {periodo['ano'] - 1} AND INMES <= {periodo['mes']}))
        """
    return "1=1"

def get_periodos_comparacao():
    """Retorna informações dos dois períodos para comparação"""
    periodo_atual = obter_periodo_referencia()
    if periodo_atual:
        return {
            'atual': {
                'ano': periodo_atual['ano'],
                'mes': periodo_atual['mes'],
                'mes_nome': periodo_atual['mes_nome'],
                'label': f"{periodo_atual['mes_nome']}/{periodo_atual['ano']}"
            },
            'anterior': {
                'ano': periodo_atual['ano'] - 1,
                'mes': periodo_atual['mes'],
                'mes_nome': periodo_atual['mes_nome'],
                'label': f"{periodo_atual['mes_nome']}/{periodo_atual['ano'] - 1}"
            },
            'titulo_comparativo': f"{periodo_atual['mes_nome']}/{periodo_atual['ano'] - 1} vs {periodo_atual['mes_nome']}/{periodo_atual['ano']}"
        }
    return None

def get_meses_disponiveis():
    """Retorna lista de todos os meses disponíveis no banco para seleção manual"""
    caminho_db = os.path.join(os.path.dirname(__file__), '../../banco_lancamento_receita.db')
    conn = sqlite3.connect(caminho_db)
    
    query = """
    SELECT DISTINCT COEXERCICIO, INMES 
    FROM (
        SELECT COEXERCICIO, INMES FROM lancamentos
        UNION
        SELECT COEXERCICIO, INMES FROM fato_saldos
    )
    ORDER BY COEXERCICIO DESC, INMES DESC
    """
    
    resultados = conn.execute(query).fetchall()
    conn.close()
    
    meses_nomes = {
        1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril",
        5: "Maio", 6: "Junho", 7: "Julho", 8: "Agosto",
        9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"
    }
    
    meses_disponiveis = []
    for ano, mes in resultados:
        meses_disponiveis.append({
            'ano': ano,
            'mes': mes,
            'mes_nome': meses_nomes.get(mes, ""),
            'valor': f"{ano}-{mes:02d}",
            'label': f"{meses_nomes.get(mes, "")}/{ano}"
        })
    
    return meses_disponiveis


# TESTE - Executar quando o arquivo for rodado diretamente
if __name__ == "__main__":
    print("=== TESTE DO MÓDULO PERÍODO ===")
    print()
    
    # Testa obter período de referência
    periodo = obter_periodo_referencia()
    
    if periodo:
        print(f"✅ Período de referência encontrado:")
        print(f"   - Mês: {periodo['mes']} ({periodo['mes_nome']})")
        print(f"   - Ano: {periodo['ano']}")
        print(f"   - Período completo: {periodo['periodo_completo']}")
        print()
        
        # Testa os filtros
        print(f"📊 Filtros SQL gerados:")
        print(f"   - Filtro acumulado atual: {get_filtro_acumulado()}")
        print(f"   - Filtro mês atual: {get_filtro_mes_atual()}")
        print(f"   - Filtro acumulado ano anterior: {get_filtro_acumulado_ano_anterior()}")
        print(f"   - Filtro mês ano anterior: {get_filtro_mes_ano_anterior()}")
        print()
        
        # Testa comparativo
        print(f"📈 Períodos de Comparação:")
        comparacao = get_periodos_comparacao()
        print(f"   - Período atual: {comparacao['atual']['label']}")
        print(f"   - Período anterior: {comparacao['anterior']['label']}")
        print(f"   - Título: {comparacao['titulo_comparativo']}")
        print()
        
        # Testa dados reais
        caminho_db = os.path.join(os.path.dirname(__file__), '../../banco_lancamento_receita.db')
        conn = sqlite3.connect(caminho_db)
        
        # Compara os dois períodos
        query_comparativo = f"""
        SELECT 
            COEXERCICIO as ano,
            COUNT(*) as total_registros,
            SUM(VALANCAMENTO) as receita_total
        FROM lancamentos 
        WHERE {get_filtro_comparativo()}
        GROUP BY COEXERCICIO
        ORDER BY COEXERCICIO
        """
        
        print(f"🔍 Dados Comparativos:")
        resultados = conn.execute(query_comparativo).fetchall()
        for ano, registros, receita in resultados:
            print(f"   - {ano}: {registros:,} lançamentos | R$ {receita:,.2f}")
        
        # Mostra crescimento
        if len(resultados) == 2:
            crescimento = ((resultados[1][2] / resultados[0][2]) - 1) * 100
            print(f"\n   📊 Crescimento: {crescimento:+.1f}%")
        
        # Lista meses disponíveis
        print(f"\n📅 Meses disponíveis para seleção:")
        meses = get_meses_disponiveis()
        for i, mes in enumerate(meses[:6]):  # Mostra só os 6 primeiros
            print(f"   - {mes['label']} (valor: {mes['valor']})")
        if len(meses) > 6:
            print(f"   ... e mais {len(meses) - 6} meses")
        
        conn.close()
        
    else:
        print("❌ Erro: Não foi possível determinar o período de referência")
        print("   Verifique se o banco de dados existe e contém dados")