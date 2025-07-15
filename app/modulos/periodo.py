# app/modulos/periodo.py
"""Gerenciamento do período de referência para relatórios - Versão híbrida"""

import sqlite3
import os
from datetime import datetime

def obter_periodo_referencia():
    """Retorna o mês e ano de referência baseado no último INMES disponível"""
    
    # Detecta se está no Railway (produção)
    if os.environ.get('RAILWAY_ENVIRONMENT') or os.environ.get('DATABASE_URL'):
        return obter_periodo_postgresql()
    else:
        return obter_periodo_sqlite()

def obter_periodo_postgresql():
    """Versão PostgreSQL para produção"""
    try:
        import psycopg2
        database_url = os.environ.get('DATABASE_URL')
        
        if not database_url:
            print("AVISO: DATABASE_URL não encontrada, usando período padrão")
            return periodo_padrao()
        
        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()
        
        # Busca o maior INMES na tabela PostgreSQL
        cursor.execute("SELECT MAX(COEXERCICIO * 100 + INMES) as mes_ano FROM fato_saldos")
        resultado = cursor.fetchone()[0]
        
        conn.close()
        
        if resultado:
            ano = resultado // 100
            mes = resultado % 100
            mes_nome = obter_nome_mes(mes)
            
            return {
                'mes': mes,
                'ano': ano,
                'mes_nome': mes_nome,
                'periodo_completo': f"{mes_nome}/{ano}"
            }
        
    except Exception as e:
        print(f"Erro PostgreSQL: {e}")
    
    return periodo_padrao()

def obter_periodo_sqlite():
    """Versão SQLite para desenvolvimento local"""
    
    # Usa o caminho do banco de saldos na estrutura local
    caminho_db = os.path.join(os.path.dirname(__file__), '../../dados/db/banco_saldo_receita.db')
    
    if not os.path.exists(caminho_db):
        # Se não encontrar, tenta o banco de lançamentos
        caminho_db = os.path.join(os.path.dirname(__file__), '../../dados/db/banco_lancamento_receita.db')
    
    if not os.path.exists(caminho_db):
        print(f"AVISO: Banco de dados não encontrado em {caminho_db}")
        return periodo_padrao()
    
    conn = sqlite3.connect(caminho_db)
    
    # Busca o maior INMES nas tabelas disponíveis
    try:
        # Tenta primeiro na tabela fato_saldos
        query = "SELECT MAX(COEXERCICIO * 100 + INMES) as mes_ano FROM fato_saldos"
        resultado = conn.execute(query).fetchone()[0]
    except:
        try:
            # Se não der, tenta na tabela lancamentos
            query = "SELECT MAX(COEXERCICIO * 100 + INMES) as mes_ano FROM lancamentos"
            resultado = conn.execute(query).fetchone()[0]
        except:
            resultado = None
    
    conn.close()
    
    if resultado:
        ano = resultado // 100
        mes = resultado % 100
        mes_nome = obter_nome_mes(mes)
        
        return {
            'mes': mes,
            'ano': ano,
            'mes_nome': mes_nome,
            'periodo_completo': f"{mes_nome}/{ano}"
        }
    
    return periodo_padrao()

def obter_nome_mes(mes):
    """Retorna o nome do mês"""
    meses_nomes = {
        1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril",
        5: "Maio", 6: "Junho", 7: "Julho", 8: "Agosto",
        9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"
    }
    return meses_nomes.get(mes, "Mês Inválido")

def periodo_padrao():
    """Retorna período padrão quando não consegue acessar dados"""
    return {
        'mes': 6,
        'ano': 2025,
        'mes_nome': 'Junho',
        'periodo_completo': 'Junho/2025'
    }

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
        filtro = f"""
        ((COEXERCICIO = {periodo['ano']} AND INMES <= {periodo['mes']}) 
        OR 
        (COEXERCICIO = {periodo['ano'] - 1} AND INMES <= {periodo['mes']}))
        """
        return filtro
    return "1=1"

def get_periodos_comparacao():
    """Retorna informações dos dois períodos para comparação"""
    periodo_atual = obter_periodo_referencia()
    if periodo_atual:
        ano_anterior = periodo_atual['ano'] - 1
        mes_nome = periodo_atual['mes_nome']
        
        return {
            'atual': {
                'ano': periodo_atual['ano'],
                'mes': periodo_atual['mes'],
                'mes_nome': mes_nome,
                'label': f"{mes_nome}/{periodo_atual['ano']}"
            },
            'anterior': {
                'ano': ano_anterior,
                'mes': periodo_atual['mes'],
                'mes_nome': mes_nome,
                'label': f"{mes_nome}/{ano_anterior}"
            },
            'titulo_comparativo': f"{mes_nome}/{ano_anterior} vs {mes_nome}/{periodo_atual['ano']}"
        }
    return None

def get_meses_disponiveis():
    """Retorna lista de todos os meses disponíveis no banco para seleção manual"""
    
    # Detecta ambiente
    if os.environ.get('RAILWAY_ENVIRONMENT') or os.environ.get('DATABASE_URL'):
        return get_meses_disponiveis_postgresql()
    else:
        return get_meses_disponiveis_sqlite()

def get_meses_disponiveis_postgresql():
    """Versão PostgreSQL"""
    try:
        import psycopg2
        database_url = os.environ.get('DATABASE_URL')
        
        if not database_url:
            return []
        
        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()
        
        cursor.execute("""
        SELECT DISTINCT COEXERCICIO, INMES, NOME_MES 
        FROM dim_tempo
        ORDER BY COEXERCICIO DESC, INMES DESC
        """)
        
        resultados = cursor.fetchall()
        conn.close()
        
        meses_disponiveis = []
        for ano, mes, nome_mes in resultados:
            valor_formatado = f"{ano}-{mes:02d}"
            label_formatado = f"{nome_mes}/{ano}"
            
            meses_disponiveis.append({
                'ano': ano,
                'mes': mes,
                'mes_nome': nome_mes,
                'valor': valor_formatado,
                'label': label_formatado
            })
        
        return meses_disponiveis
        
    except Exception as e:
        print(f"Erro ao obter meses PostgreSQL: {e}")
        return []

def get_meses_disponiveis_sqlite():
    """Versão SQLite"""
    # Tenta primeiro o banco de saldos
    caminho_db = os.path.join(os.path.dirname(__file__), '../../dados/db/banco_saldo_receita.db')
    
    if not os.path.exists(caminho_db):
        caminho_db = os.path.join(os.path.dirname(__file__), '../../dados/db/banco_lancamento_receita.db')
    
    if not os.path.exists(caminho_db):
        return []
    
    conn = sqlite3.connect(caminho_db)
    
    try:
        # Usa a tabela dim_tempo que foi criada nos conversores
        query = """
        SELECT DISTINCT COEXERCICIO, INMES, NOME_MES 
        FROM dim_tempo
        ORDER BY COEXERCICIO DESC, INMES DESC
        """
        
        resultados = conn.execute(query).fetchall()
        conn.close()
        
        meses_disponiveis = []
        for ano, mes, nome_mes in resultados:
            valor_formatado = f"{ano}-{mes:02d}"
            label_formatado = f"{nome_mes}/{ano}"
            
            meses_disponiveis.append({
                'ano': ano,
                'mes': mes,
                'mes_nome': nome_mes,
                'valor': valor_formatado,
                'label': label_formatado
            })
        
        return meses_disponiveis
        
    except Exception as e:
        print(f"Erro ao obter meses SQLite: {e}")
        conn.close()
        return []