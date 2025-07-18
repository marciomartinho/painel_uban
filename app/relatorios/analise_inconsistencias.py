# app/relatorios/analise_inconsistencias.py

import pandas as pd
import locale
from ..modulos.conexao_hibrida import ConexaoBanco, get_db_environment, adaptar_query

try:
    locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
except locale.Error:
    print("Localidade pt_BR.UTF-8 não encontrada, usando localidade padrão.")

def _formatar_moeda(valor):
    if valor is None: return "R$ 0,00"
    return locale.currency(valor, grouping=True, symbol=True)

def _executar_query(query, params=None):
    """Função auxiliar para executar queries em modo híbrido."""
    with ConexaoBanco() as conn:
        query_adaptada = adaptar_query(query)
        # O read_sql_query do Pandas lida com os placeholders de forma diferente
        if get_db_environment() == 'postgres':
             df = pd.read_sql_query(query_adaptada, conn, params=params)
        else:
             df = pd.read_sql_query(query_adaptada.replace('%s', '?'), conn, params=params or [])
        
        # Padroniza os nomes das colunas para minúsculas
        df.columns = [col.lower() for col in df.columns]
        return df

def obter_exercicios_disponiveis():
    try:
        query = "SELECT DISTINCT coexercicio FROM fato_saldos ORDER BY coexercicio DESC;"
        df = _executar_query(query)
        return df['coexercicio'].tolist()
    except Exception as e:
        print(f"Erro ao obter exercícios: {e}")
        return [2025, 2024]

def analisar_fontes_superavit(exercicio):
    try:
        query = "SELECT coug, cocontacontabil, cofonte, saldo_contabil FROM fato_saldos WHERE coexercicio = %s;"
        df = _executar_query(query, params=(exercicio,))
        
        if df.empty: return []
        
        df['cofonte'] = df['cofonte'].astype(str)
        df_superavit = df[df['cofonte'].str.match(r'^[348]')].copy()
        
        if df_superavit.empty: return []
        
        agrupado = df_superavit.groupby(['coug', 'cocontacontabil', 'cofonte']).agg(saldo_total=('saldo_contabil', 'sum')).reset_index()
        inconsistencias = agrupado[agrupado['saldo_total'] != 0].copy()
        inconsistencias['saldo_formatado'] = inconsistencias['saldo_total'].apply(_formatar_moeda)
        return inconsistencias.to_dict('records')
    except Exception as e:
        print(f"Erro na análise de fontes de superávit: {e}")
        return []

def analisar_ugs_invalidas(exercicio):
    try:
        type_cast = "::text" if get_db_environment() == 'postgres' else ""
        df_ugs = _executar_query(f"SELECT coug, noug FROM dimensoes.unidades_gestoras")
        df_ugs['coug'] = df_ugs['coug'].astype(str)

        query_saldos = f"SELECT coug, cocontacontabil, cocontacorrente, saldo_contabil FROM fato_saldos WHERE coexercicio = %s AND intipoadm = 1 AND coug{type_cast} != '130101';"
        df_saldos = _executar_query(query_saldos, params=(exercicio,))
        
        if df_saldos.empty: return []

        agrupado = df_saldos.groupby(['coug', 'cocontacontabil', 'cocontacorrente']).agg(saldo_total=('saldo_contabil', 'sum')).reset_index()
        inconsistencias = agrupado[agrupado['saldo_total'] != 0].copy()
        
        if inconsistencias.empty: return []
        
        inconsistencias['coug'] = inconsistencias['coug'].astype(str)
        resultado_final = pd.merge(inconsistencias, df_ugs, on='coug', how='left')
        resultado_final['noug'] = resultado_final['noug'].fillna('Nome da UG não encontrado')
        resultado_final['saldo_formatado'] = resultado_final['saldo_total'].apply(_formatar_moeda)
        return resultado_final.to_dict('records')
    except Exception as e:
        print(f"Erro na análise de UGs inválidas: {e}")
        return []

def analisar_saldos_negativos(exercicio):
    try:
        df_ugs = _executar_query("SELECT coug, noug FROM dimensoes.unidades_gestoras")
        df_ugs['coug'] = df_ugs['coug'].astype(str)

        query = "SELECT coug, cocontacontabil, cocontacorrente, saldo_contabil FROM fato_saldos WHERE coexercicio = %s AND cocontacontabil = '621200000';"
        df = _executar_query(query, params=(exercicio,))

        if df.empty: return []

        agrupado = df.groupby(['coug', 'cocontacontabil', 'cocontacorrente']).agg(saldo_total=('saldo_contabil', 'sum')).reset_index()
        inconsistencias = agrupado[agrupado['saldo_total'] < 0].copy()
        
        if inconsistencias.empty: return []
        
        inconsistencias['coug'] = inconsistencias['coug'].astype(str)
        resultado_final = pd.merge(inconsistencias, df_ugs, on='coug', how='left')
        resultado_final['noug'] = resultado_final['noug'].fillna('Nome da UG não encontrado')
        resultado_final['saldo_formatado'] = resultado_final['saldo_total'].apply(_formatar_moeda)
        sorted_records = resultado_final.sort_values(by='saldo_total', ascending=True).to_dict('records')
        return sorted_records
    except Exception as e:
        print(f"Erro na análise de saldos negativos: {e}")
        return []