# app/routes_relatorios.py
"""Rotas para os relatórios do sistema"""

from flask import Blueprint, render_template
import sqlite3
import os
from app.modulos.periodo import obter_periodo_referencia, get_periodos_comparacao
from app.modulos.formatacao import FormatadorMonetario

relatorios_bp = Blueprint('relatorios', __name__, url_prefix='/relatorios')

def get_db_connection():
    """Cria conexão com o banco de dados"""
    db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'banco_lancamento_receita.db')
    return sqlite3.connect(db_path)

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
    
    # Conecta ao banco
    conn = get_db_connection()
    conn.row_factory = sqlite3.Row
    
    # Query para buscar dados agregados por categoria, origem e espécie
    query = """
    WITH dados_agregados AS (
        SELECT 
            CATEGORIARECEITA,
            ORIGEM,
            ESPECIE,
            COEXERCICIO,
            INMES,
            
            -- Categorias
            cat.NOCATEGORIARECEITA,
            -- Origens
            ori.NOFONTERECEITA,
            -- Especies
            esp.NOSUBFONTERECEITA,
            
            -- Valores agregados
            SUM("PREVISAO INICIAL") as previsao_inicial,
            SUM("PREVISAO ATUALIZADA LIQUIDA") as previsao_atualizada,
            SUM("RECEITA LIQUIDA") as receita_realizada
            
        FROM fato_saldos s
        LEFT JOIN categorias cat ON s.CATEGORIARECEITA = cat.COCATEGORIARECEITA
        LEFT JOIN origens ori ON s.ORIGEM = ori.COFONTERECEITA
        LEFT JOIN especies esp ON s.ESPECIE = esp.COSUBFONTERECEITA
        
        WHERE INMES = ? AND COEXERCICIO IN (?, ?)
        
        GROUP BY CATEGORIARECEITA, ORIGEM, ESPECIE, COEXERCICIO, INMES,
                 NOCATEGORIARECEITA, NOFONTERECEITA, NOSUBFONTERECEITA
    )
    SELECT * FROM dados_agregados
    ORDER BY CATEGORIARECEITA, ORIGEM, ESPECIE, COEXERCICIO
    """
    
    # Executa query
    cursor = conn.execute(query, (
        periodo['mes'],
        periodo['ano'] - 1,
        periodo['ano']
    ))
    
    resultados = cursor.fetchall()
    conn.close()
    
    # Processa dados para estrutura hierárquica
    dados_hierarquicos = processar_hierarquia(resultados, periodo['ano'])
    
    return render_template(
        'relatorios/balanco_orcamentario_receita.html',
        dados=dados_hierarquicos,
        periodo=periodo,
        periodos=periodos,
        fmt=FormatadorMonetario
    )

def processar_hierarquia(resultados, ano_atual):
    """Processa resultados do banco em estrutura hierárquica"""
    
    # Dicionários para agregação
    categorias = {}
    origens = {}
    especies = {}
    
    # Processa cada linha
    for row in resultados:
        cat_id = row['CATEGORIARECEITA']
        ori_id = row['ORIGEM']
        esp_id = row['ESPECIE']
        ano = row['COEXERCICIO']
        
        # Inicializa categoria se não existe
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
        
        # Inicializa origem se não existe
        origem_key = f"{cat_id}-{ori_id}"
        if origem_key not in origens:
            origens[origem_key] = {
                'id': f'ori-{origem_key}',
                'codigo': ori_id,
                'descricao': row['NOFONTERECEITA'] or f'Origem {ori_id}',
                'nivel': 1,
                'pai': f'cat-{cat_id}',
                'previsao_inicial': 0,
                'previsao_atualizada': 0,
                'receita_atual': 0,
                'receita_anterior': 0
            }
        
        # Inicializa espécie se não existe
        especie_key = f"{cat_id}-{ori_id}-{esp_id}"
        if especie_key not in especies:
            especies[especie_key] = {
                'id': f'esp-{especie_key}',
                'codigo': esp_id,
                'descricao': row['NOSUBFONTERECEITA'] or f'Espécie {esp_id}',
                'nivel': 2,
                'pai': f'ori-{origem_key}',
                'previsao_inicial': 0,
                'previsao_atualizada': 0,
                'receita_atual': 0,
                'receita_anterior': 0
            }
        
        # Acumula valores
        if ano == ano_atual:
            # Espécie
            especies[especie_key]['previsao_inicial'] += row['previsao_inicial'] or 0
            especies[especie_key]['previsao_atualizada'] += row['previsao_atualizada'] or 0
            especies[especie_key]['receita_atual'] += row['receita_realizada'] or 0
            
            # Origem
            origens[origem_key]['previsao_inicial'] += row['previsao_inicial'] or 0
            origens[origem_key]['previsao_atualizada'] += row['previsao_atualizada'] or 0
            origens[origem_key]['receita_atual'] += row['receita_realizada'] or 0
            
            # Categoria
            categorias[cat_id]['previsao_inicial'] += row['previsao_inicial'] or 0
            categorias[cat_id]['previsao_atualizada'] += row['previsao_atualizada'] or 0
            categorias[cat_id]['receita_atual'] += row['receita_realizada'] or 0
        else:
            # Ano anterior
            especies[especie_key]['receita_anterior'] += row['receita_realizada'] or 0
            origens[origem_key]['receita_anterior'] += row['receita_realizada'] or 0
            categorias[cat_id]['receita_anterior'] += row['receita_realizada'] or 0
    
    # Calcula variações e monta lista final
    dados_finais = []
    
    # Adiciona total geral primeiro
    total_geral = {
        'id': 'total',
        'codigo': '',
        'descricao': 'RECEITAS CORRENTES',
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
    
    # Processa cada nível calculando variações
    for items in [categorias.values(), origens.values(), especies.values()]:
        for item in items:
            item['variacao_absoluta'] = item['receita_atual'] - item['receita_anterior']
            item['variacao_percentual'] = (
                (item['variacao_absoluta'] / item['receita_anterior']) 
                if item['receita_anterior'] != 0 else 0
            )
            dados_finais.append(item)
    
    # Ordena por hierarquia
    return sorted(dados_finais, key=lambda x: (
        x['nivel'],
        x['pai'] or '',
        x['codigo']
    ))

# Registra o filtro no template
@relatorios_bp.app_template_filter('formatar_codigo_descricao')
def formatar_codigo_descricao(codigo, descricao):
    """Formata código e descrição para exibição"""
    if codigo:
        # Remove espaços extras da descrição
        desc_limpa = ' '.join(descricao.split()) if descricao else ''
        return f"{codigo} - {desc_limpa}"
    return descricao