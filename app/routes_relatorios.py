# app/routes_relatorios.py
"""Rotas para os relatórios do sistema"""

from flask import Blueprint, render_template
import sqlite3
import os
from app.modulos.periodo import obter_periodo_referencia, get_periodos_comparacao
from app.modulos.formatacao import FormatadorMonetario

relatorios_bp = Blueprint('relatorios', __name__, url_prefix='/relatorios')

def get_db_connections():
    """Cria conexões com os bancos de dados"""
    base_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'dados', 'db')
    
    connections = {
        'saldos': os.path.join(base_path, 'banco_saldo_receita.db'),
        'lancamentos': os.path.join(base_path, 'banco_lancamento_receita.db'),
        'dimensoes': os.path.join(base_path, 'banco_dimensoes.db')
    }
    
    # Verifica quais bancos existem
    for key, path in connections.items():
        if not os.path.exists(path):
            print(f"AVISO: Banco {key} não encontrado em {path}")
            connections[key] = None
    
    return connections

@relatorios_bp.route('/')
def index():
    """Página principal dos relatórios"""
    periodo = obter_periodo_referencia()
    return render_template('relatorios/index.html', periodo=periodo)

@relatorios_bp.route('/balanco-orcamentario-receita')
def balanco_orcamentario_receita():
    """Relatório Balanço Orçamentário da Receita"""
    
    # Obtém informações do período
    periodo = obter_periodo_referencia()
    periodos = get_periodos_comparacao()
    
    print(f"\n=== DEBUG BALANCO ORCAMENTARIO ===")
    print(f"Período atual: {periodo}")
    print(f"Períodos comparação: {periodos}")
    
    # Obtém caminhos dos bancos
    db_paths = get_db_connections()
    
    if not db_paths['saldos']:
        return render_template('erro.html', 
                             mensagem="Banco de dados de saldos não encontrado. Execute os conversores primeiro.")
    
    # Conecta ao banco de saldos
    conn = sqlite3.connect(db_paths['saldos'])
    conn.row_factory = sqlite3.Row
    
    # Primeiro, vamos verificar que dados temos para o período
    cursor = conn.cursor()
    cursor.execute("""
        SELECT DISTINCT COEXERCICIO, INMES 
        FROM fato_saldos 
        ORDER BY COEXERCICIO DESC, INMES DESC
        LIMIT 10
    """)
    periodos_disponiveis = cursor.fetchall()
    print(f"\nPeríodos disponíveis no banco:")
    for p in periodos_disponiveis:
        print(f"  - {p['INMES']}/{p['COEXERCICIO']}")
    
    # Query simplificada - vamos buscar TODOS os dados primeiro para debug
    query_debug = """
    SELECT 
        CATEGORIARECEITA,
        NOCATEGORIARECEITA,
        COEXERCICIO,
        INMES,
        COUNT(*) as total_registros,
        SUM(COALESCE("PREVISAO INICIAL", 0)) as previsao_inicial,
        SUM(COALESCE("PREVISAO ATUALIZADA LIQUIDA", 0)) as previsao_atualizada,
        SUM(COALESCE("RECEITA LIQUIDA", 0)) as receita_realizada
    FROM fato_saldos
    WHERE INMES = ? AND COEXERCICIO = ?
    GROUP BY CATEGORIARECEITA, NOCATEGORIARECEITA, COEXERCICIO, INMES
    """
    
    print(f"\nBuscando dados para: MÊS={periodo['mes']}, ANO={periodo['ano']}")
    
    try:
        # Busca dados do ano atual
        cursor = conn.execute(query_debug, (periodo['mes'], periodo['ano']))
        dados_atual = cursor.fetchall()
        
        print(f"\nDados encontrados para {periodo['mes']}/{periodo['ano']}: {len(dados_atual)} categorias")
        for row in dados_atual:
            print(f"  - Categoria {row['CATEGORIARECEITA']}: {row['total_registros']} registros, Receita: R$ {row['receita_realizada']:,.2f}")
        
        # Busca dados do ano anterior
        cursor = conn.execute(query_debug, (periodo['mes'], periodo['ano'] - 1))
        dados_anterior = cursor.fetchall()
        
        print(f"\nDados encontrados para {periodo['mes']}/{periodo['ano']-1}: {len(dados_anterior)} categorias")
        
        # Se não encontrou dados, vamos tentar o último período disponível
        if not dados_atual and not dados_anterior:
            print("\nNenhum dado encontrado para o período solicitado. Buscando último período disponível...")
            
            cursor.execute("""
                SELECT MAX(COEXERCICIO * 100 + INMES) as ultimo_periodo
                FROM fato_saldos
            """)
            ultimo = cursor.fetchone()
            if ultimo and ultimo['ultimo_periodo']:
                ano_ultimo = ultimo['ultimo_periodo'] // 100
                mes_ultimo = ultimo['ultimo_periodo'] % 100
                
                print(f"Usando período: {mes_ultimo}/{ano_ultimo}")
                
                # Atualiza período para usar o último disponível
                periodo['mes'] = mes_ultimo
                periodo['ano'] = ano_ultimo
                
                # Busca novamente com o período correto
                cursor = conn.execute(query_debug, (mes_ultimo, ano_ultimo))
                dados_atual = cursor.fetchall()
                
                cursor = conn.execute(query_debug, (mes_ultimo, ano_ultimo - 1))
                dados_anterior = cursor.fetchall()
        
        # Processa os dados para a estrutura esperada
        dados_hierarquicos = processar_dados_simples(dados_atual, dados_anterior, periodo['ano'])
        
        # Se ainda não tiver dados, usa exemplo
        if not dados_hierarquicos:
            print("\nUsando dados de exemplo...")
            dados_hierarquicos = gerar_dados_exemplo(periodo)
        
    except Exception as e:
        print(f"\nERRO ao executar query: {e}")
        import traceback
        traceback.print_exc()
        dados_hierarquicos = gerar_dados_exemplo(periodo)
    
    finally:
        conn.close()
    
    print(f"\nTotal de linhas para exibir: {len(dados_hierarquicos)}")
    print("=== FIM DEBUG ===\n")
    
    return render_template(
        'relatorios/balanco_orcamentario_receita.html',
        dados=dados_hierarquicos,
        periodo=periodo,
        periodos=periodos,
        fmt=FormatadorMonetario
    )

def processar_dados_simples(dados_atual, dados_anterior, ano_atual):
    """Processa dados de forma simplificada apenas por categoria"""
    
    # Dicionário para armazenar dados por categoria
    categorias = {}
    
    # Processa dados do ano atual
    for row in dados_atual:
        cat_id = str(row['CATEGORIARECEITA'])
        if cat_id not in categorias:
            categorias[cat_id] = {
                'id': f'cat-{cat_id}',
                'codigo': cat_id,
                'descricao': row['NOCATEGORIARECEITA'] or f'Categoria {cat_id}',
                'nivel': 0,
                'pai': None,
                'previsao_inicial': 0,
                'previsao_atualizada': 0,
                'receita_atual': 0,
                'receita_anterior': 0
            }
        
        categorias[cat_id]['previsao_inicial'] += row['previsao_inicial'] or 0
        categorias[cat_id]['previsao_atualizada'] += row['previsao_atualizada'] or 0
        categorias[cat_id]['receita_atual'] += row['receita_realizada'] or 0
    
    # Processa dados do ano anterior
    for row in dados_anterior:
        cat_id = str(row['CATEGORIARECEITA'])
        if cat_id not in categorias:
            categorias[cat_id] = {
                'id': f'cat-{cat_id}',
                'codigo': cat_id,
                'descricao': row['NOCATEGORIARECEITA'] or f'Categoria {cat_id}',
                'nivel': 0,
                'pai': None,
                'previsao_inicial': 0,
                'previsao_atualizada': 0,
                'receita_atual': 0,
                'receita_anterior': 0
            }
        
        categorias[cat_id]['receita_anterior'] += row['receita_realizada'] or 0
    
    # Se não há dados, retorna vazio
    if not categorias:
        return []
    
    # Calcula variações e monta lista final
    dados_finais = []
    
    # Calcula totais
    total_geral = {
        'id': 'total',
        'codigo': '',
        'descricao': 'TOTAL GERAL',
        'nivel': -1,
        'pai': None,
        'previsao_inicial': sum(cat['previsao_inicial'] for cat in categorias.values()),
        'previsao_atualizada': sum(cat['previsao_atualizada'] for cat in categorias.values()),
        'receita_atual': sum(cat['receita_atual'] for cat in categorias.values()),
        'receita_anterior': sum(cat['receita_anterior'] for cat in categorias.values())
    }
    
    total_geral['variacao_absoluta'] = total_geral['receita_atual'] - total_geral['receita_anterior']
    total_geral['variacao_percentual'] = (
        (total_geral['variacao_absoluta'] / total_geral['receita_anterior']) 
        if total_geral['receita_anterior'] != 0 else 0
    )
    
    dados_finais.append(total_geral)
    
    # Adiciona categorias com variações calculadas
    for cat in categorias.values():
        cat['variacao_absoluta'] = cat['receita_atual'] - cat['receita_anterior']
        cat['variacao_percentual'] = (
            (cat['variacao_absoluta'] / cat['receita_anterior']) 
            if cat['receita_anterior'] != 0 else 0
        )
        dados_finais.append(cat)
    
    return dados_finais

def processar_hierarquia(resultados, ano_atual):
    """Processa resultados do banco em estrutura hierárquica (versão original)"""
    # ... código anterior mantido ...
    # Esta função será usada quando implementarmos a versão completa com origem e espécie
    pass

def gerar_dados_exemplo(periodo):
    """Gera dados de exemplo para teste do relatório"""
    return [
        {
            'id': 'total',
            'codigo': '',
            'descricao': 'TOTAL GERAL (DADOS DE EXEMPLO)',
            'nivel': -1,
            'pai': None,
            'previsao_inicial': 48535054229.00,
            'previsao_atualizada': 48535054229.00,
            'receita_atual': 22982101363.93,
            'receita_anterior': 27528439126.69,
            'variacao_absoluta': -4546337762.76,
            'variacao_percentual': -0.165
        },
        {
            'id': 'cat-1',
            'codigo': '1',
            'descricao': 'RECEITAS CORRENTES',
            'nivel': 0,
            'pai': None,
            'previsao_inicial': 45000000000.00,
            'previsao_atualizada': 45000000000.00,
            'receita_atual': 21000000000.00,
            'receita_anterior': 25000000000.00,
            'variacao_absoluta': -4000000000.00,
            'variacao_percentual': -0.16
        },
        {
            'id': 'cat-2',
            'codigo': '2',
            'descricao': 'RECEITAS DE CAPITAL',
            'nivel': 0,
            'pai': None,
            'previsao_inicial': 3535054229.00,
            'previsao_atualizada': 3535054229.00,
            'receita_atual': 1982101363.93,
            'receita_anterior': 2528439126.69,
            'variacao_absoluta': -546337762.76,
            'variacao_percentual': -0.216
        }
    ]

# Registra o filtro no template
@relatorios_bp.app_template_filter('formatar_codigo_descricao')
def formatar_codigo_descricao(codigo, descricao):
    """Formata código e descrição para exibição"""
    if codigo:
        # Remove espaços extras da descrição
        desc_limpa = ' '.join(descricao.split()) if descricao else ''
        return f"{codigo} - {desc_limpa}"
    return descricao