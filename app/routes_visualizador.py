# app/routes_visualizador.py
"""Rotas para visualização direta dos dados dos bancos"""

from flask import Blueprint, render_template, request, jsonify, abort
import pandas as pd
from io import BytesIO
import os
import sqlite3
import traceback
from app.modulos.conexao_hibrida import ConexaoBanco, get_db_environment, adaptar_query
import psycopg2.extras

visualizador_bp = Blueprint('visualizador', __name__, url_prefix='/visualizador')

# --- Funções Auxiliares Híbridas ---

def get_table_list(cursor, db_name):
    """Retorna a lista de tabelas para um grupo lógico de banco de dados."""
    if get_db_environment() == 'postgres':
        schema_filter = "'dimensoes'" if db_name == 'dimensoes' else "'public'"
        cursor.execute(f"""
            SELECT table_name as name, table_type as type, table_schema as schema
            FROM information_schema.tables
            WHERE table_schema = {schema_filter}
            ORDER BY table_type, table_name;
        """)
    else: # sqlite
        cursor.execute("""
            SELECT name, type, 'main' as schema FROM sqlite_master
            WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%'
            ORDER BY type, name;
        """)
    return cursor.fetchall()

def get_table_info(cursor, table_name, schema):
    """Obtém informações de colunas e contagem de registros."""
    full_table_name = f'"{schema}"."{table_name}"' if get_db_environment() == 'postgres' else table_name
    
    if get_db_environment() == 'postgres':
        cursor.execute("""
            SELECT column_name as name, data_type as type
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position;
        """, (schema, table_name))
        colunas_raw = cursor.fetchall()
        colunas = [{'nome': col['name'], 'tipo': col['type']} for col in colunas_raw]
    else: # sqlite
        cursor.execute(f"PRAGMA table_info({table_name})")
        colunas_raw = cursor.fetchall()
        colunas = [{'nome': col['name'], 'tipo': col['type']} for col in colunas_raw]
    
    cursor.execute(adaptar_query(f"SELECT COUNT(*) FROM {full_table_name}"))
    total_registros = cursor.fetchone()[0]
    
    return {'colunas': colunas, 'total_registros': total_registros}

# --- Rotas ---

@visualizador_bp.route('/')
def index():
    # Esta função já está corrigida e funcionando
    env = get_db_environment()
    status_bancos = {
        'saldos': {'existe': False, 'tabelas': 0},
        'lancamentos': {'existe': False, 'tabelas': 0},
        'dimensoes': {'existe': False, 'tabelas': 0}
    }
    try:
        if env == 'postgres':
            with ConexaoBanco() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                cursor.execute("SELECT table_name, table_schema FROM information_schema.tables WHERE table_schema IN ('public', 'dimensoes')")
                tabelas = cursor.fetchall()
                
                for t in tabelas:
                    if t['table_name'] == 'fato_saldos': status_bancos['saldos']['existe'] = True
                    if t['table_name'] == 'lancamentos': status_bancos['lancamentos']['existe'] = True
                    if t['table_schema'] == 'dimensoes': status_bancos['dimensoes']['existe'] = True
                
                if status_bancos['saldos']['existe']: status_bancos['saldos']['tabelas'] = len([t for t in tabelas if t['table_name'] in ['fato_saldos', 'dim_tempo'] and t['table_schema'] == 'public'])
                if status_bancos['lancamentos']['existe']: status_bancos['lancamentos']['tabelas'] = len([t for t in tabelas if t['table_name'] == 'lancamentos' and t['table_schema'] == 'public'])
                if status_bancos['dimensoes']['existe']: status_bancos['dimensoes']['tabelas'] = len([t for t in tabelas if t['table_schema'] == 'dimensoes'])
        else: # sqlite
            base_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'dados', 'db')
            db_map = {
                'saldos': 'banco_saldo_receita.db',
                'lancamentos': 'banco_lancamento_receita.db',
                'dimensoes': 'banco_dimensoes.db'
            }
            for banco, filename in db_map.items():
                db_path = os.path.join(base_path, filename)
                if os.path.exists(db_path):
                    try:
                        conn = sqlite3.connect(db_path)
                        cursor = conn.cursor()
                        cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
                        status_bancos[banco] = {'existe': True, 'tabelas': cursor.fetchone()[0]}
                        conn.close()
                    except Exception as e:
                        print(f"Erro ao verificar banco SQLite {banco}: {e}")
    except Exception as e:
        print(f"Erro ao verificar status dos bancos: {e}")
    return render_template('visualizador/index.html', status_bancos=status_bancos)


@visualizador_bp.route('/estrutura/<db_name>')
def estrutura_banco(db_name):
    estrutura = {}
    try:
        with ConexaoBanco(db_name) as conn:
            if get_db_environment() == 'postgres':
                cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            else:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()

            tabelas_raw = get_table_list(cursor, db_name)
            
            for t_row in tabelas_raw:
                t = dict(t_row)
                info = get_table_info(cursor, t['name'], t.get('schema', 'main'))
                info['tipo'] = t['type'].lower()
                estrutura[t['name']] = info
    except Exception as e:
        traceback.print_exc()
        return render_template('erro.html', mensagem=f"Erro ao buscar estrutura do banco: {e}")
    return render_template('visualizador/estrutura.html', db_name=db_name, estrutura=estrutura)


@visualizador_bp.route('/dados/<db_name>/<table_name>')
def visualizar_dados(db_name, table_name):
    page = request.args.get('page', 1, type=int)
    per_page = 100 # Define per_page aqui
    offset = (page - 1) * per_page
    
    try:
        with ConexaoBanco(db_name) as conn:
            if get_db_environment() == 'postgres':
                cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            else:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()

            # CORREÇÃO: Determina o schema correto
            schema = 'dimensoes' if db_name == 'dimensoes' else 'public'
            full_table_name = f'"{schema}"."{table_name}"' if get_db_environment() == 'postgres' else table_name

            info = get_table_info(cursor, table_name, schema)
            colunas = [c['nome'] for c in info['colunas']]
            total_registros = info['total_registros']

            query = f"SELECT * FROM {full_table_name} LIMIT {per_page} OFFSET {offset}"
            query_adaptada = adaptar_query(query)
            cursor.execute(query_adaptada)
            dados = cursor.fetchall()

            total_pages = (total_registros + per_page - 1) // per_page
            
            # CORREÇÃO: Passa 'per_page' para o template
            return render_template('visualizador/dados.html',
                                 db_name=db_name, table_name=table_name, colunas=colunas, dados=dados,
                                 page=page, total_pages=total_pages, total_registros=total_registros,
                                 per_page=per_page,  # <-- AQUI ESTÁ A CORREÇÃO
                                 valores_unicos={}, campos_filtro=[])
    except Exception as e:
        traceback.print_exc()
        return render_template('erro.html', mensagem=f"Erro ao visualizar dados: {e}")

@visualizador_bp.route('/query/<db_name>', methods=['GET', 'POST'])
def executar_query(db_name):
    # Esta função já está corrigida
    tabelas_disponiveis = []
    try:
        with ConexaoBanco(db_name) as conn:
            if get_db_environment() == 'postgres':
                cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                tabelas_raw = get_table_list(cursor, db_name)
                tabelas_disponiveis = [f"{t['schema']}.{t['name']}" for t in tabelas_raw]
            else:
                cursor = conn.cursor()
                tabelas_raw = get_table_list(cursor, db_name)
                tabelas_disponiveis = [t[0] for t in tabelas_raw]
    except Exception as e:
        print(f"Erro ao listar tabelas para query: {e}")
    
    if request.method == 'POST':
        # ... (código do POST continua igual)
        query = request.form.get('query', '').strip()
        if not query.upper().startswith('SELECT'):
            return render_template('visualizador/query.html', db_name=db_name, query=query, erro="Apenas queries SELECT são permitidas.", tabelas_disponiveis=tabelas_disponiveis)
        try:
            with ConexaoBanco(db_name) as conn:
                if get_db_environment() == 'postgres':
                    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                else:
                    conn.row_factory = sqlite3.Row
                    cursor = conn.cursor()
                
                query_adaptada = adaptar_query(query)
                cursor.execute(query_adaptada)
                dados = cursor.fetchall()
                colunas = [desc[0] for desc in cursor.description] if dados else []
                return render_template('visualizador/query_result.html',
                                     db_name=db_name, query=query, colunas=colunas, dados=dados,
                                     total_registros=len(dados))
        except Exception as e:
            return render_template('visualizador/query.html', db_name=db_name, query=query, erro=str(e), tabelas_disponiveis=tabelas_disponiveis)
    
    return render_template('visualizador/query.html', db_name=db_name, tabelas_disponiveis=tabelas_disponiveis)

@visualizador_bp.route('/exportar/<db_name>/<table_name>')
def exportar_dados(db_name, table_name):
    # Esta função já está corrigida
    try:
        with ConexaoBanco(db_name) as conn:
            schema = 'dimensoes' if db_name == 'dimensoes' else 'public'
            full_table_name = f'"{schema}"."{table_name}"' if get_db_environment() == 'postgres' else table_name
            df = pd.read_sql_query(f"SELECT * FROM {full_table_name}", conn)
            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name=table_name[:31], index=False)
            output.seek(0)
            return send_file(
                output,
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                as_attachment=True,
                download_name=f"{db_name}_{table_name}.xlsx"
            )
    except Exception as e:
        traceback.print_exc()
        return render_template('erro.html', mensagem=f"Erro ao exportar dados: {e}")