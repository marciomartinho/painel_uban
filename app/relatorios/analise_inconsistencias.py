# app/relatorios/analise_inconsistencias.py

import pandas as pd
import sqlite3
import locale
import os

# Caminhos para os bancos de dados
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(BASE_DIR, 'dados', 'db', 'banco_saldo_receita.db')
DIMENSOES_DB_PATH = os.path.join(BASE_DIR, 'dados', 'db', 'banco_dimensoes.db')

try:
    locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
except locale.Error:
    print("Localidade pt_BR.UTF-8 não encontrada, usando localidade padrão.")

def _conectar_db(path):
    """Conecta a um banco de dados específico pelo seu caminho."""
    conn = sqlite3.connect(path)
    return conn

def formatar_moeda(valor):
    if valor is None: return "R$ 0,00"
    return locale.currency(valor, grouping=True, symbol=True)

def obter_exercicios_disponiveis():
    try:
        conn = _conectar_db(DB_PATH)
        query = "SELECT DISTINCT COEXERCICIO FROM fato_saldos ORDER BY COEXERCICIO DESC;"
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df['COEXERCICIO'].tolist()
    except Exception as e:
        print(f"Erro ao obter exercícios: {e}")
        return [2025, 2024]

def analisar_fontes_superavit(exercicio):
    try:
        conn = _conectar_db(DB_PATH)
        query = "SELECT COUG, COCONTACONTABIL, COFONTE, saldo_contabil FROM fato_saldos WHERE COEXERCICIO = ?;"
        df = pd.read_sql_query(query, conn, params=(exercicio,))
        conn.close()
        if df.empty: return []
        df['COFONTE'] = df['COFONTE'].astype(str)
        df_superavit = df[df['COFONTE'].str.match(r'^[348]')].copy()
        if df_superavit.empty: return []
        agrupado = df_superavit.groupby(['COUG', 'COCONTACONTABIL', 'COFONTE']).agg(saldo_total=('saldo_contabil', 'sum')).reset_index()
        inconsistencias = agrupado[agrupado['saldo_total'] != 0].copy()
        inconsistencias['saldo_formatado'] = inconsistencias['saldo_total'].apply(formatar_moeda)
        return inconsistencias.to_dict('records')
    except Exception as e:
        print(f"Erro na análise de fontes de superávit: {e}")
        return []

def analisar_ugs_invalidas(exercicio):
    try:
        conn_dim = _conectar_db(DIMENSOES_DB_PATH)
        df_ugs = pd.read_sql_query("SELECT COUG, NOUG FROM unidades_gestoras", conn_dim)
        df_ugs['COUG'] = df_ugs['COUG'].astype(str)
        conn_dim.close()

        conn_saldos = _conectar_db(DB_PATH)
        query = "SELECT COUG, COCONTACONTABIL, COCONTACORRENTE, saldo_contabil FROM fato_saldos WHERE COEXERCICIO = ? AND INTIPOADM = 1 AND COUG != '130101';"
        df_saldos = pd.read_sql_query(query, conn_saldos, params=(exercicio,))
        conn_saldos.close()
        
        if df_saldos.empty: return []

        agrupado = df_saldos.groupby(['COUG', 'COCONTACONTABIL', 'COCONTACORRENTE']).agg(saldo_total=('saldo_contabil', 'sum')).reset_index()
        inconsistencias = agrupado[agrupado['saldo_total'] != 0].copy()
        if inconsistencias.empty: return []
        inconsistencias['COUG'] = inconsistencias['COUG'].astype(str)
        resultado_final = pd.merge(inconsistencias, df_ugs, on='COUG', how='left')
        resultado_final['NOUG'].fillna('Nome da UG não encontrado', inplace=True)
        resultado_final['saldo_formatado'] = resultado_final['saldo_total'].apply(formatar_moeda)
        return resultado_final.to_dict('records')
    except Exception as e:
        print(f"Erro na análise de UGs inválidas: {e}")
        return []

def analisar_saldos_negativos(exercicio):
    try:
        conn_dim = _conectar_db(DIMENSOES_DB_PATH)
        df_ugs = pd.read_sql_query("SELECT COUG, NOUG FROM unidades_gestoras", conn_dim)
        df_ugs['COUG'] = df_ugs['COUG'].astype(str)
        conn_dim.close()

        conn = _conectar_db(DB_PATH)
        
        # <<< MUDANÇA >>> Adicionado o filtro para a conta contábil específica
        query = """
            SELECT COUG, COCONTACONTABIL, COCONTACORRENTE, saldo_contabil 
            FROM fato_saldos 
            WHERE COEXERCICIO = ? AND COCONTACONTABIL = '621200000';
        """
        df = pd.read_sql_query(query, conn, params=(exercicio,))
        conn.close()

        if df.empty:
            return []

        agrupado = df.groupby(['COUG', 'COCONTACONTABIL', 'COCONTACORRENTE']).agg(
            saldo_total=('saldo_contabil', 'sum')
        ).reset_index()
        
        inconsistencias = agrupado[agrupado['saldo_total'] < 0].copy()
        
        if inconsistencias.empty:
            return []
        
        inconsistencias['COUG'] = inconsistencias['COUG'].astype(str)

        resultado_final = pd.merge(inconsistencias, df_ugs, on='COUG', how='left')
        resultado_final['NOUG'].fillna('Nome da UG não encontrado', inplace=True)
            
        resultado_final['saldo_formatado'] = resultado_final['saldo_total'].apply(formatar_moeda)
        return resultado_final.sort_values(by='saldo_total', ascending=True).to_dict('records')
        
    except Exception as e:
        print(f"Erro na análise de saldos negativos: {e}")
        return []