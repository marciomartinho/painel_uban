# app/modulos/exportador_html.py
"""
Módulo para exportação de relatórios em formato HTML - Versão Reescrita
Gera HTML limpo sem elementos interativos
"""

import os
import re
from datetime import datetime
from flask import render_template_string, current_app
import base64
from bs4 import BeautifulSoup


class ExportadorHTML:
    """Classe para exportar relatórios em HTML estático"""
    
    def __init__(self):
        self.elementos_remover = [
            'script',
            'button',
            'select',
            'input',
            'textarea',
            'form',
            '.btn',
            '.action-bar',
            '.action-buttons',
            '.filter-section',
            '.modal',
            '.modal-overlay',
            '.loading',
            '.spinner',
            '.no-print',
            '.no-export'
        ]
        
        self.atributos_remover = [
            'onclick',
            'onchange',
            'onsubmit',
            'onmouseover',
            'onmouseout',
            'ondblclick',
            'onkeydown',
            'onkeyup',
            'onkeypress'
        ]
    
    def get_estilos_base(self):
        """Retorna estilos CSS base para exportação"""
        return """
        /* Reset e Estilos Base para Exportação */
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
            -webkit-print-color-adjust: exact !important;
            print-color-adjust: exact !important;
        }
        
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, sans-serif;
            line-height: 1.6;
            color: #333;
            background: white;
            padding: 20px;
            max-width: 1400px;
            margin: 0 auto;
        }
        
        /* Elementos removidos */
        button, .btn, select, input, .action-bar, .filter-section, 
        .modal, .loading, .spinner, .no-print, .no-export {
            display: none !important;
        }
        
        /* Links desabilitados */
        a {
            text-decoration: none !important;
            color: inherit !important;
            cursor: default !important;
            pointer-events: none !important;
        }
        
        /* Tabelas */
        table {
            width: 100%;
            border-collapse: collapse;
            margin: 1rem 0;
            page-break-inside: avoid;
        }
        
        th, td {
            padding: 0.75rem;
            text-align: left;
            border-bottom: 1px solid #dee2e6;
        }
        
        th {
            background-color: #f8f9fa;
            font-weight: 600;
            color: #495057;
        }
        
        tr:hover {
            background-color: transparent !important;
        }
        
        /* Cards e Containers */
        .card, .modern-card {
            background: white;
            border: 1px solid #dee2e6;
            border-radius: 0.5rem;
            padding: 1.5rem;
            margin-bottom: 1.5rem;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
            page-break-inside: avoid;
        }
        
        /* Headers */
        h1, h2, h3, h4, h5, h6 {
            margin-bottom: 1rem;
            font-weight: 600;
            line-height: 1.2;
            color: #212529;
            page-break-after: avoid;
        }
        
        /* Valores coloridos */
        .valor-positivo { color: #28a745 !important; }
        .valor-negativo { color: #dc3545 !important; }
        
        /* Cabeçalho de exportação */
        .export-header {
            background: #f8f9fa;
            border: 1px solid #dee2e6;
            padding: 1rem;
            margin-bottom: 2rem;
            border-radius: 0.375rem;
            font-size: 0.875rem;
            color: #6c757d;
        }
        
        .export-header strong {
            color: #495057;
            font-size: 1rem;
        }
        
        /* Rodapé de exportação */
        .export-footer {
            margin-top: 3rem;
            padding-top: 1.5rem;
            border-top: 2px solid #dee2e6;
            text-align: center;
            font-size: 0.75rem;
            color: #6c757d;
        }
        
        /* Impressão */
        @media print {
            body {
                padding: 0;
                margin: 0;
            }
            
            .export-header,
            .export-footer {
                display: none;
            }
            
            .page-break {
                page-break-before: always;
            }
            
            table {
                font-size: 0.875rem;
            }
        }
        
        /* Remove interatividade */
        * {
            cursor: default !important;
            user-select: text !important;
        }
        
        .toggle-btn {
            display: none !important;
        }
        """
    
    def limpar_html(self, html_content):
        """
        Limpa o HTML removendo elementos interativos
        
        Args:
            html_content: String HTML original
            
        Returns:
            String HTML limpo
        """
        # Parse HTML com BeautifulSoup
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Remove elementos por seletor
        for seletor in self.elementos_remover:
            if seletor.startswith('.'):
                # Seletor de classe
                elementos = soup.find_all(class_=seletor[1:])
            else:
                # Seletor de tag
                elementos = soup.find_all(seletor)
            
            for elemento in elementos:
                elemento.decompose()
        
        # Remove elementos com classes específicas
        for elemento in soup.find_all(True):
            if elemento.get('class'):
                classes = elemento.get('class')
                if any('btn' in cls or 'button' in cls for cls in classes):
                    elemento.decompose()
                    continue
        
        # Remove atributos de eventos
        for elemento in soup.find_all(True):
            for atributo in self.atributos_remover:
                if elemento.has_attr(atributo):
                    del elemento[atributo]
        
        # Converte links em spans (mantém o texto)
        for link in soup.find_all('a'):
            if 'btn' in link.get('class', []) or 'button' in link.get('class', []):
                link.decompose()
            else:
                # Mantém o conteúdo mas remove a funcionalidade
                span = soup.new_tag('span')
                span.string = link.get_text()
                if link.get('class'):
                    span['class'] = link.get('class')
                link.replace_with(span)
        
        return str(soup)
    
    def processar_html_para_download(self, html_content, titulo="Relatório", metadata=None):
        """
        Processa HTML completo para download
        
        Args:
            html_content: Conteúdo HTML do relatório
            titulo: Título do documento
            metadata: Dicionário com metadados (periodo, coug, etc)
            
        Returns:
            String HTML completa
        """
        # Limpa o HTML
        html_limpo = self.limpar_html(html_content)
        
        # Extrai apenas o conteúdo relevante (remove HTML, HEAD, BODY tags)
        soup = BeautifulSoup(html_limpo, 'html.parser')
        body_content = soup.find('body')
        if body_content:
            conteudo_principal = str(body_content.decode_contents())
        else:
            conteudo_principal = html_limpo
        
        # Remove qualquer referência a "Preparando download"
        conteudo_principal = re.sub(
            r'<[^>]*>Preparando download[^<]*</[^>]*>', 
            '', 
            conteudo_principal, 
            flags=re.IGNORECASE
        )
        
        # Monta metadados
        data_hora = datetime.now().strftime('%d/%m/%Y às %H:%M')
        info_metadata = []
        
        if metadata:
            if metadata.get('periodo'):
                info_metadata.append(f"Período: {metadata['periodo']}")
            if metadata.get('coug'):
                info_metadata.append(f"Unidade: {metadata['coug']}")
            if metadata.get('filtro'):
                info_metadata.append(f"Filtro: {metadata['filtro']}")
        
        # Monta HTML completo
        html_completo = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta name="description" content="{titulo} - Exportado do Sistema">
    <title>{titulo}</title>
    <style>
{self.get_estilos_base()}
    </style>
</head>
<body>
    <div class="export-header">
        <strong>{titulo}</strong><br>
        Documento exportado em: {data_hora}
        {('<br>' + ' | '.join(info_metadata)) if info_metadata else ''}
    </div>
    
    <div class="content-wrapper">
{conteudo_principal}
    </div>
    
    <div class="export-footer">
        Este documento foi gerado automaticamente pelo sistema e representa uma visualização estática dos dados no momento da exportação.
    </div>
</body>
</html>"""
        
        return html_completo
    
    def gerar_nome_arquivo(self, tipo_relatorio, periodo=None, filtros=None):
        """
        Gera nome padronizado para o arquivo
        
        Args:
            tipo_relatorio: Tipo do relatório
            periodo: Dicionário com ano e mês
            filtros: Dicionário com filtros aplicados
            
        Returns:
            String com nome do arquivo
        """
        partes = [tipo_relatorio]
        
        if periodo:
            if periodo.get('ano') and periodo.get('mes'):
                partes.append(f"{periodo['ano']}_{periodo['mes']:02d}")
        
        if filtros:
            if filtros.get('coug'):
                partes.append(f"coug_{filtros['coug']}")
            if filtros.get('filtro'):
                partes.append(filtros['filtro'])
        
        # Adiciona timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        partes.append(timestamp)
        
        # Limpa caracteres especiais
        nome = '_'.join(filter(None, partes))
        nome = re.sub(r'[^\w\-_]', '_', nome)
        
        return f"{nome}.html"


# Funções auxiliares para uso direto

def exportar_relatorio_html(conteudo_html, tipo_relatorio, **kwargs):
    """
    Função auxiliar simplificada para exportar relatório
    
    Args:
        conteudo_html: HTML do relatório
        tipo_relatorio: Tipo do relatório
        **kwargs: Argumentos adicionais
    
    Returns:
        Tupla (html_completo, nome_arquivo)
    """
    exportador = ExportadorHTML()
    
    # Extrai parâmetros
    titulo = kwargs.get('titulo', f'Relatório {tipo_relatorio}')
    periodo = kwargs.get('periodo')
    filtros = kwargs.get('filtros', {})
    
    # Monta metadata
    metadata = {
        'periodo': f"{periodo['ano']}/{periodo['mes']:02d}" if periodo else None,
        'coug': filtros.get('coug'),
        'filtro': filtros.get('filtro')
    }
    
    # Processa o HTML
    html_completo = exportador.processar_html_para_download(
        conteudo_html,
        titulo=titulo,
        metadata={k: v for k, v in metadata.items() if v}
    )
    
    # Gera nome do arquivo
    nome_arquivo = exportador.gerar_nome_arquivo(tipo_relatorio, periodo, filtros)
    
    return html_completo, nome_arquivo


def limpar_html_para_exportacao(html):
    """
    Função rápida para limpar HTML
    
    Args:
        html: String HTML
        
    Returns:
        String HTML limpo
    """
    exportador = ExportadorHTML()
    return exportador.limpar_html(html)