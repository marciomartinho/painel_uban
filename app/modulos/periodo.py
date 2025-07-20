# app/modulos/periodo.py
"""Gerenciamento do período de referência para relatórios - Versão híbrida"""

from datetime import datetime
from app.modulos.conexao_hibrida import ConexaoBanco, adaptar_query, get_db_environment

# Cache simples para evitar múltiplas leituras do banco na mesma requisição
_cache_periodo = None

def obter_periodo_referencia(force_reload=False):
    """Retorna o mês e ano de referência baseado no último INMES disponível."""
    global _cache_periodo
    if _cache_periodo and not force_reload:
        return _cache_periodo

    try:
        with ConexaoBanco() as conn:
            cursor = conn.cursor()
            
            # CORREÇÃO: Adiciona type cast para PostgreSQL
            if get_db_environment() == 'postgres':
                query = "SELECT MAX(COEXERCICIO::integer * 100 + INMES::integer) as mes_ano FROM fato_saldos"
            else:
                query = "SELECT MAX(COEXERCICIO * 100 + INMES) as mes_ano FROM fato_saldos"
                
            query_adaptada = adaptar_query(query)
            
            try:
                cursor.execute(query_adaptada)
                resultado = cursor.fetchone()
                # Em psycopg2 (RealDictCursor), o resultado é um dict. Em sqlite3.Row, pode ser acessado por índice ou nome.
                res_val = resultado['mes_ano'] if isinstance(resultado, dict) else resultado[0]
            except Exception:
                # Se falhar, tenta na tabela de lançamentos (para SQLite local)
                if get_db_environment() == 'postgres':
                    query = "SELECT MAX(COEXERCICIO::integer * 100 + INMES::integer) as mes_ano FROM lancamentos"
                else:
                    query = "SELECT MAX(COEXERCICIO * 100 + INMES) as mes_ano FROM lancamentos_db.lancamentos"
                    
                query_adaptada = adaptar_query(query)
                cursor.execute(query_adaptada)
                resultado = cursor.fetchone()
                res_val = resultado['mes_ano'] if isinstance(resultado, dict) else resultado[0]

            if res_val:
                ano = int(res_val) // 100
                mes = int(res_val) % 100
                mes_nome = obter_nome_mes(mes)
                
                _cache_periodo = {
                    'mes': mes,
                    'ano': ano,
                    'mes_nome': mes_nome,
                    'periodo_completo': f"{mes_nome}/{ano}"
                }
                return _cache_periodo

    except Exception as e:
        print(f"AVISO: Não foi possível determinar o período a partir do banco de dados: {e}. Usando período padrão.")

    _cache_periodo = periodo_padrao()
    return _cache_periodo

def obter_nome_mes(mes):
    """Retorna o nome do mês."""
    meses_nomes = {
        1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril",
        5: "Maio", 6: "Junho", 7: "Julho", 8: "Agosto",
        9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"
    }
    return meses_nomes.get(mes, "Mês Inválido")

def periodo_padrao():
    """Retorna período padrão quando não consegue acessar dados."""
    return {
        'mes': datetime.now().month,
        'ano': datetime.now().year,
        'mes_nome': obter_nome_mes(datetime.now().month),
        'periodo_completo': f"{obter_nome_mes(datetime.now().month)}/{datetime.now().year}"
    }