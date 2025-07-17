// app/static/js/download-html.js
/**
 * Sistema de download de relatórios em HTML - Versão Corrigida
 * Captura seções específicas da página, incluindo gráficos como imagens estáticas.
 */

(function() {
    'use strict';
    
    // Converte todos os canvas dentro de uma área em imagens DataURL
    async function capturarGraficosComoImagens(areaElement) {
        const canvasElements = areaElement.querySelectorAll('canvas');
        const promessas = Array.from(canvasElements).map(async (canvas) => {
            if (!canvas.id) {
                canvas.id = `canvas-export-${Math.random().toString(36).substring(2, 9)}`;
            }
            await new Promise(resolve => setTimeout(resolve, 200)); 
            return {
                id: canvas.id,
                dataURL: canvas.toDataURL('image/png')
            };
        });
        
        const resultados = await Promise.all(promessas);
        const graficosMap = new Map();
        resultados.forEach(res => graficosMap.set(res.id, res.dataURL));
        return graficosMap;
    }

    // Limpa o HTML clonado para exportação
    function limparHTMLParaExportacao(elementoClonado, graficosMap) {
        elementoClonado.querySelectorAll('.no-export, .btn, button, select, input, .toggle-btn').forEach(el => el.remove());

        elementoClonado.querySelectorAll('*').forEach(el => {
            for (const attr of el.attributes) {
                if (attr.name.startsWith('on')) {
                    el.removeAttribute(attr.name);
                }
            }
        });

        graficosMap.forEach((dataURL, id) => {
            const canvas = elementoClonado.querySelector(`#${id}`);
            if (canvas) {
                const img = document.createElement('img');
                img.src = dataURL;
                img.style.display = 'block';
                img.style.maxWidth = '100%';
                img.style.height = 'auto';
                canvas.parentNode.replaceChild(img, canvas);
            }
        });

        return elementoClonado.innerHTML;
    }

    // Gera o HTML final para o arquivo
    function gerarHTMLFinal(titulo, conteudo, css, opcoes) {
        const dataHora = new Date().toLocaleString('pt-BR');
        
        // Constrói o subtítulo dinamicamente com os detalhes do filtro
        let subtitulo = `<p>Documento gerado em: ${dataHora}</p>`;
        const detalhesFiltro = [];
        if (opcoes.cougNome && opcoes.cougNome !== 'Consolidado') {
            detalhesFiltro.push(`<strong>Unidade Gestora:</strong> ${opcoes.cougNome}`);
        }
        // <<< MUDANÇA: Remove o texto "Filtro:" e deixa apenas o valor em negrito >>>
        if (opcoes.filtroDescricao && opcoes.filtroDescricao !== 'Todas as Receitas') {
            detalhesFiltro.push(`<strong>${opcoes.filtroDescricao}</strong>`);
        }
        
        if (detalhesFiltro.length > 0) {
            subtitulo += `<div class="export-subtitle">${detalhesFiltro.join(' &nbsp; | &nbsp; ')}</div>`;
        }

        return `<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <title>${titulo}</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif; background-color: #eef2f5; margin: 0; padding: 0; }
        .content-wrapper { max-width: 1400px; margin: 20px auto; padding: 20px; background-color: #ffffff; border-radius: 8px; box-shadow: 0 4px 15px rgba(0,0,0,0.05); }
        .info-card, .table-section, .modern-card { margin: 20px auto !important; }
        
        .export-header {
            background: linear-gradient(135deg, #003366 0%, #004a99 100%);
            color: white;
            padding: 30px 40px;
            text-align: center;
            border-bottom: 5px solid #ffc107;
        }
        .export-header .icon {
            font-size: 48px; margin-bottom: 15px; display: inline-block;
            width: 60px; height: 60px; line-height: 60px;
            background-color: rgba(255, 255, 255, 0.1);
            border-radius: 50%;
        }
        .export-header h1 { font-size: 32px; font-weight: 700; margin: 0 0 10px 0; letter-spacing: -0.5px; }
        .export-header p { font-size: 16px; opacity: 0.9; margin: 0; }
        .export-subtitle {
            /* <<< MUDANÇA: Aumenta o tamanho da fonte do subtítulo >>> */
            font-size: 1.1em;
            margin-top: 15px;
            padding-top: 15px;
            border-top: 1px solid rgba(255, 255, 255, 0.2);
            opacity: 0.9;
        }

        .export-footer { text-align: center; margin-top: 30px; padding-top: 15px; border-top: 1px solid #ccc; font-size: 12px; color: #777; }
        
        ${css}
    </style>
</head>
<body>
    <div class="export-header">
        <div class="icon">📄</div>
        <h1>${titulo}</h1>
        ${subtitulo}
    </div>
    <div class="content-wrapper">
        ${conteudo}
    </div>
    <div class="export-footer">
        Relatório gerado automaticamente pelo sistema.
    </div>
</body>
</html>`;
    }

    // Função para baixar o conteúdo como arquivo
    function baixarArquivo(conteudo, nomeArquivo) {
        const blob = new Blob([conteudo], { type: 'text/html;charset=utf-8' });
        const link = document.createElement('a');
        link.href = URL.createObjectURL(blob);
        link.download = nomeArquivo;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        URL.revokeObjectURL(link.href);
    }
    
    // Função principal de download
    async function executarDownload(opcoes) {
        const { areaId, prefixo, titulo } = opcoes;
        const areaParaExportar = document.getElementById(areaId);

        if (!areaParaExportar) {
            console.error(`Área de exportação com ID "${areaId}" não encontrada.`);
            alert('Erro: Não foi possível encontrar a área para exportar.');
            return;
        }

        try {
            const graficosMap = await capturarGraficosComoImagens(areaParaExportar);
            const areaClonada = areaParaExportar.cloneNode(true);
            const conteudoLimpo = limparHTMLParaExportacao(areaClonada, graficosMap);
            const estilosPagina = Array.from(document.styleSheets)
                .map(sheet => {
                    try {
                        return Array.from(sheet.cssRules).map(rule => rule.cssText).join('');
                    } catch (e) { return ''; }
                }).join('');
            
            const htmlFinal = gerarHTMLFinal(titulo, conteudoLimpo, estilosPagina, opcoes);
            
            const nomeArquivo = `${prefixo}_${new Date().toISOString().slice(0, 16).replace(/[-:T]/g, '')}.html`;
            baixarArquivo(htmlFinal, nomeArquivo);

        } catch (e) {
            console.error('Erro ao gerar o download:', e);
            alert('Ocorreu um erro ao gerar o arquivo HTML. Verifique o console para mais detalhes.');
        }
    }

    // Expõe a funcionalidade para o objeto window
    window.DownloadHTML = {
        baixarBalancoOrcamentario: function(params = {}) {
            executarDownload({
                areaId: 'area-exportavel',
                prefixo: 'balanco_orcamentario_receita',
                titulo: 'Balanço Orçamentário da Receita',
                cougNome: params.cougNome,
                filtroDescricao: params.filtroDescricao
            });
        }
    };

})();