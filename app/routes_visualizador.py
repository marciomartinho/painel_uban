# app/routes_visualizador.py
"""Rotas para visualização direta dos dados dos bancos"""

from flask import Blueprint, render_template, request, jsonify
import sqlite3
import os
import pandas as pd

visualizador_bp = Blueprint('visualizador', __name__, url_prefix='/visualizador')

def get_db_path(db_name):
    """Retorna o caminho do banco de dados"""
    base_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'dados', 'db')
    
    db_paths = {
        'saldos': os.path.join(base_path, 'banco_saldo_receita.db'),
        'lancamentos': os.path.join(base_path, 'banco_lancamento_receita.db'),
        'dimensoes': os.path.join(base_path, 'banco_dimensoes.db')
    }
    
    return db_paths.get(db_name)

@visualizador_bp.route('/')
def index():
    """Página principal do visualizador"""
    return render_template('visualizador/index.html')

@visualizador_bp.route('/estrutura/<db_name>')
def estrutura_banco(db_name):
    """Mostra a estrutura do banco de dados"""
    db_path = get_db_path(db_name)
    
    if not db_path or not os.path.exists(db_path):
        return jsonify({'erro': f'Banco {db_name} não encontrado'}), 404
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Lista todas as tabelas
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tabelas = cursor.fetchall()
    
    estrutura = {}
    for tabela in tabelas:
        nome_tabela = tabela[0]
        # Pega informações das colunas
        cursor.execute(f"PRAGMA table_info({nome_tabela})")
        colunas = cursor.fetchall()
        
        # Conta registros
        cursor.execute(f"SELECT COUNT(*) FROM {nome_tabela}")
        total_registros = cursor.fetchone()[0]
        
        estrutura[nome_tabela] = {
            'colunas': [{'nome': col[1], 'tipo': col[2]} for col in colunas],
            'total_registros': total_registros
        }
    
    conn.close()
    
    return render_template('visualizador/estrutura.html', 
                         db_name=db_name, 
                         estrutura=estrutura)

@visualizador_bp.route('/dados/<db_name>/<table_name>')
def visualizar_dados(db_name, table_name):
    """Visualiza dados de uma tabela específica"""
    db_path = get_db_path(db_name)
    
    if not db_path or not os.path.exists(db_path):
        return jsonify({'erro': f'Banco {db_name} não encontrado'}), 404
    
    # Parâmetros de paginação
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 100, type=int)
    offset = (page - 1) * per_page
    
    # Construção dinâmica de filtros
    where_clauses = []
    params = []
    
    # Define campos de filtro específicos por tabela
    if table_name == 'lancamentos':
        campos_filtro = ['COEXERCICIO', 'COUG', 'COEVENTO', 'COCONTACONTABIL', 
                        'COCONTACORRENTE', 'INMES', 'COUGDESTINO', 'COUGCONTAB', 
                        'CATEGORIARECEITA', 'COFONTERECEITA', 'COSUBFONTERECEITA', 
                        'CORUBRICA', 'COALINE', 'COFONTE']
    elif table_name == 'fato_saldos':
        campos_filtro = ['CATEGORIA', 'ORIGEM', 'ESPECIE', 'ESPECIFICACAO', 
                        'ALINEA', 'COEXERCICIO', 'INMES', 'COUG', 
                        'COCONTACORRENTE', 'INTIPOADM', 'NOUG']
    else:
        # Para outras tabelas, usa todos os parâmetros recebidos
        campos_filtro = [key for key in request.args.keys() if key not in ['page', 'per_page']]
    
    # Aplica filtros
    for campo in campos_filtro:
        valor = request.args.get(campo)
        if valor:
            where_clauses.append(f"{campo} = ?")
            params.append(valor)
    
    # Monta cláusula WHERE
    where_clause = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""
    
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    
    try:
        # Query para contar total de registros com filtros
        count_query = f"SELECT COUNT(*) as total FROM {table_name} {where_clause}"
        cursor = conn.execute(count_query, params)
        total_registros = cursor.fetchone()['total']
        
        # Query para buscar dados paginados
        data_query = f"""
        SELECT * FROM {table_name} 
        {where_clause}
        LIMIT ? OFFSET ?
        """
        params_paginated = params + [per_page, offset]
        
        cursor = conn.execute(data_query, params_paginated)
        dados = cursor.fetchall()
        
        # Pega nomes das colunas
        colunas = [description[0] for description in cursor.description]
        
        # Converte para lista de dicionários
        dados_lista = []
        for row in dados:
            dados_lista.append(dict(row))
        
        # Busca valores únicos para os campos de filtro relevantes
        valores_unicos = {}
        
        # Limita a busca de valores únicos apenas aos campos relevantes
        campos_unicos = campos_filtro if table_name in ['lancamentos', 'fato_saldos'] else []
        
        for campo in campos_unicos:
            if campo in colunas:
                try:
                    query_unique = f"""
                    SELECT DISTINCT {campo} 
                    FROM {table_name} 
                    WHERE {campo} IS NOT NULL
                    ORDER BY {campo}
                    LIMIT 100
                    """
                    cursor = conn.execute(query_unique)
                    valores = [row[0] for row in cursor.fetchall()]
                    if valores:
                        valores_unicos[campo] = valores
                except:
                    pass
        
    except Exception as e:
        conn.close()
        return jsonify({'erro': str(e)}), 500
    
    finally:
        conn.close()
    
    # Calcula informações de paginação
    total_pages = (total_registros + per_page - 1) // per_page
    
    return render_template('visualizador/dados.html',
                         db_name=db_name,
                         table_name=table_name,
                         colunas=colunas,
                         dados=dados_lista,
                         page=page,
                         per_page=per_page,
                         total_registros=total_registros,
                         total_pages=total_pages,
                         valores_unicos=valores_unicos)

@visualizador_bp.route('/query/<db_name>', methods=['GET', 'POST'])
def executar_query(db_name):
    """Permite executar queries SQL customizadas (apenas SELECT)"""
    if request.method == 'GET':
        return render_template('visualizador/query.html', db_name=db_name)
    
    query = request.form.get('query', '').strip()
    
    # Validação básica de segurança - apenas permite SELECT
    if not query.upper().startswith('SELECT'):
        return jsonify({'erro': 'Apenas queries SELECT são permitidas'}), 400
    
    # Palavras proibidas
    palavras_proibidas = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'CREATE']
    for palavra in palavras_proibidas:
        if palavra in query.upper():
            return jsonify({'erro': f'Query contém comando proibido: {palavra}'}), 400
    
    db_path = get_db_path(db_name)
    if not db_path or not os.path.exists(db_path):
        return jsonify({'erro': f'Banco {db_name} não encontrado'}), 404
    
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    
    try:
        cursor = conn.execute(query)
        dados = cursor.fetchall()
        colunas = [description[0] for description in cursor.description] if cursor.description else []
        
        # Converte para lista de dicionários
        dados_lista = []
        for row in dados:
            dados_lista.append(dict(row))
        
        conn.close()
        
        return render_template('visualizador/query_result.html',
                             db_name=db_name,
                             query=query,
                             colunas=colunas,
                             dados=dados_lista,
                             total_registros=len(dados_lista))
        
    except Exception as e:
        conn.close()
        return render_template('visualizador/query.html', 
                             db_name=db_name, 
                             query=query,
                             erro=str(e))

@visualizador_bp.route('/exportar/<db_name>/<table_name>')
def exportar_dados(db_name, table_name):
    """Exporta dados para Excel"""
    db_path = get_db_path(db_name)
    
    if not db_path or not os.path.exists(db_path):
        return jsonify({'erro': f'Banco {db_name} não encontrado'}), 404
    
    # Usa pandas para facilitar a exportação
    conn = sqlite3.connect(db_path)
    
    # Filtros
    where_clause = ""
    params = []
    
    exercicio = request.args.get('exercicio', type=int)
    if exercicio:
        where_clause = f"WHERE COEXERCICIO = {exercicio}"
    
    mes = request.args.get('mes', type=int)
    if mes:
        if where_clause:
            where_clause += f" AND INMES = {mes}"
        else:
            where_clause = f"WHERE INMES = {mes}"
    
    query = f"SELECT * FROM {table_name} {where_clause}"
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    # Cria arquivo Excel em memória
    from io import BytesIO
    output = BytesIO()
    
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name=table_name[:31], index=False)
    
    output.seek(0)
    
    from flask import send_file
    return send_file(
        output,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=f'{db_name}_{table_name}_{exercicio or "todos"}_{mes or "todos"}.xlsx'
    )