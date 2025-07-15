# app/routes_visualizador.py
"""Rotas para visualização direta dos dados dos bancos"""

from flask import Blueprint, render_template, request, jsonify
import sqlite3
import os
import pandas as pd

visualizador_bp = Blueprint('visualizador', __name__, url_prefix='/visualizador')

# Tabelas conhecidas do sistema (para identificação)
TABELAS_DIMENSAO_CONHECIDAS = [
    'categorias', 'origens', 'especies', 'especificacoes', 
    'alineas', 'fontes', 'contas', 'unidades_gestoras'
]

def get_db_path(db_name):
    """Retorna o caminho do banco de dados"""
    base_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'dados', 'db')
    
    db_paths = {
        'saldos': os.path.join(base_path, 'banco_saldo_receita.db'),
        'lancamentos': os.path.join(base_path, 'banco_lancamento_receita.db'),
        'dimensoes': os.path.join(base_path, 'banco_dimensoes.db')
    }
    
    return db_paths.get(db_name)

def get_table_info(cursor, table_name):
    """Obtém informações detalhadas sobre uma tabela"""
    # Informações das colunas
    cursor.execute(f"PRAGMA table_info({table_name})")
    colunas = cursor.fetchall()
    
    # Conta registros
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        total_registros = cursor.fetchone()[0]
    except:
        total_registros = 0
    
    return {
        'colunas': [{'nome': col[1], 'tipo': col[2]} for col in colunas],
        'total_registros': total_registros
    }

@visualizador_bp.route('/')
def index():
    """Página principal do visualizador"""
    # Verifica status dos bancos
    status_bancos = {}
    for banco in ['saldos', 'lancamentos', 'dimensoes']:
        db_path = get_db_path(banco)
        if db_path and os.path.exists(db_path):
            try:
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'")
                num_tabelas = cursor.fetchone()[0]
                conn.close()
                status_bancos[banco] = {'existe': True, 'tabelas': num_tabelas}
            except:
                status_bancos[banco] = {'existe': True, 'tabelas': 0}
        else:
            status_bancos[banco] = {'existe': False, 'tabelas': 0}
    
    return render_template('visualizador/index.html', status_bancos=status_bancos)

@visualizador_bp.route('/estrutura/<db_name>')
def estrutura_banco(db_name):
    """Mostra a estrutura do banco de dados"""
    db_path = get_db_path(db_name)
    
    if not db_path or not os.path.exists(db_path):
        return jsonify({'erro': f'Banco {db_name} não encontrado'}), 404
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Lista todas as tabelas e views
    cursor.execute("""
        SELECT name, type FROM sqlite_master 
        WHERE type IN ('table', 'view') 
        AND name NOT LIKE 'sqlite_%'
        ORDER BY type, name
    """)
    objetos = cursor.fetchall()
    
    estrutura = {}
    for nome, tipo in objetos:
        info = get_table_info(cursor, nome)
        info['tipo'] = tipo
        estrutura[nome] = info
    
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
    
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    
    try:
        # Primeiro, descobre todas as colunas da tabela
        cursor = conn.execute(f"PRAGMA table_info({table_name})")
        colunas_info = cursor.fetchall()
        colunas = [col[1] for col in colunas_info]
        
        # Define campos de filtro baseado no tipo de tabela
        if table_name == 'lancamentos':
            campos_filtro = ['COEXERCICIO', 'COUG', 'COEVENTO', 'COCONTACONTABIL', 
                           'COCONTACORRENTE', 'INMES', 'COUGDESTINO', 'COUGCONTAB', 
                           'CATEGORIARECEITA', 'COFONTERECEITA', 'COSUBFONTERECEITA', 
                           'CORUBRICA', 'COALINE', 'COFONTE']
        elif table_name == 'fato_saldos':
            campos_filtro = ['CATEGORIA', 'ORIGEM', 'ESPECIE', 'ESPECIFICACAO', 
                           'ALINEA', 'COEXERCICIO', 'INMES', 'COUG', 
                           'COCONTACORRENTE', 'INTIPOADM', 'NOUG']
        elif db_name == 'dimensoes':
            # Para banco de dimensões, usa campos mais relevantes
            if table_name in TABELAS_DIMENSAO_CONHECIDAS:
                # Para tabelas conhecidas, usa a chave primária e descrição
                campos_filtro = [col for col in colunas if col.startswith('CO') or col.startswith('NO')][:5]
            else:
                # Para tabelas novas, usa os primeiros 5 campos
                campos_filtro = colunas[:5]
        else:
            # Para outras tabelas, usa os primeiros 10 campos
            campos_filtro = colunas[:10]
        
        # Garante que apenas campos existentes sejam usados
        campos_filtro = [campo for campo in campos_filtro if campo in colunas]
        
        # Aplica filtros
        for campo in campos_filtro:
            valor = request.args.get(campo)
            if valor:
                where_clauses.append(f"{campo} = ?")
                params.append(valor)
        
        # Monta cláusula WHERE
        where_clause = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        
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
        
        # Converte para lista de dicionários
        dados_lista = []
        for row in dados:
            dados_lista.append(dict(row))
        
        # Busca valores únicos para os campos de filtro
        valores_unicos = {}
        
        for campo in campos_filtro:
            try:
                # Limita a 100 valores únicos por campo
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
                         valores_unicos=valores_unicos,
                         campos_filtro=campos_filtro)

@visualizador_bp.route('/query/<db_name>', methods=['GET', 'POST'])
def executar_query(db_name):
    """Permite executar queries SQL customizadas (apenas SELECT)"""
    if request.method == 'GET':
        # Lista tabelas disponíveis para ajudar o usuário
        db_path = get_db_path(db_name)
        tabelas_disponiveis = []
        
        if db_path and os.path.exists(db_path):
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type IN ('table', 'view') 
                AND name NOT LIKE 'sqlite_%'
                ORDER BY name
            """)
            tabelas_disponiveis = [row[0] for row in cursor.fetchall()]
            conn.close()
        
        return render_template('visualizador/query.html', 
                             db_name=db_name,
                             tabelas_disponiveis=tabelas_disponiveis)
    
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
        # Busca tabelas para mostrar na página de erro
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type IN ('table', 'view') 
            AND name NOT LIKE 'sqlite_%'
            ORDER BY name
        """)
        tabelas_disponiveis = [row[0] for row in cursor.fetchall()]
        conn.close()
        
        return render_template('visualizador/query.html', 
                             db_name=db_name, 
                             query=query,
                             erro=str(e),
                             tabelas_disponiveis=tabelas_disponiveis)

@visualizador_bp.route('/exportar/<db_name>/<table_name>')
def exportar_dados(db_name, table_name):
    """Exporta dados para Excel com filtros aplicados"""
    db_path = get_db_path(db_name)
    
    if not db_path or not os.path.exists(db_path):
        return jsonify({'erro': f'Banco {db_name} não encontrado'}), 404
    
    # Constrói query com filtros
    where_clauses = []
    params = []
    
    # Aplica todos os filtros da URL
    for key, value in request.args.items():
        if key not in ['page', 'per_page'] and value:
            where_clauses.append(f"{key} = ?")
            params.append(value)
    
    where_clause = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""
    
    # Usa pandas para facilitar a exportação
    conn = sqlite3.connect(db_path)
    query = f"SELECT * FROM {table_name} {where_clause}"
    
    try:
        df = pd.read_sql_query(query, conn, params=params)
    except:
        # Fallback sem parâmetros nomeados
        query_final = query
        for param in params:
            query_final = query_final.replace('?', f"'{param}'", 1)
        df = pd.read_sql_query(query_final, conn)
    
    conn.close()
    
    # Cria arquivo Excel em memória
    from io import BytesIO
    output = BytesIO()
    
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name=table_name[:31], index=False)
        
        # Adiciona informações sobre filtros em uma segunda aba
        if where_clauses:
            filtros_df = pd.DataFrame({
                'Filtro': [key for key in request.args.keys() if key not in ['page', 'per_page'] and request.args.get(key)],
                'Valor': [value for key, value in request.args.items() if key not in ['page', 'per_page'] and value]
            })
            filtros_df.to_excel(writer, sheet_name='Filtros Aplicados', index=False)
    
    output.seek(0)
    
    from flask import send_file
    
    # Nome do arquivo com indicação de filtros
    filename_parts = [db_name, table_name]
    if where_clauses:
        filename_parts.append('filtrado')
    filename_parts.append(pd.Timestamp.now().strftime('%Y%m%d_%H%M%S'))
    
    return send_file(
        output,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=f"{'_'.join(filename_parts)}.xlsx"
    )