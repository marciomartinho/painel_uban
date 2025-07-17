// app/static/js/download-html.js
/**
 * Sistema de download de relatórios em HTML - Versão Corrigida
 * Captura a página com gráficos como imagem estática
 */

(function() {
    'use strict';
    
    // Função auxiliar para aguardar
    function aguardar(ms) {
        return new Promise(function(resolve) {
            setTimeout(resolve, ms);
        });
    }
    
    // Remove elementos de loading
    function removerLoadings() {
        try {
            var elementos = document.querySelectorAll('.loading-download, .loading, .spinner, [class*="loading"], .modal-overlay, .overlay');
            for (var i = 0; i < elementos.length; i++) {
                elementos[i].style.display = 'none';
                if (elementos[i].parentNode) {
                    elementos[i].parentNode.removeChild(elementos[i]);
                }
            }
        } catch (e) {
            console.warn('Erro ao remover loadings:', e);
        }
    }
    
    // Captura gráficos Chart.js
    function capturarGraficos() {
        return new Promise(function(resolve) {
            try {
                var graficos = {};
                var canvasElements = document.querySelectorAll('canvas');
                
                if (canvasElements.length === 0) {
                    resolve(graficos);
                    return;
                }
                
                var processados = 0;
                
                for (var i = 0; i < canvasElements.length; i++) {
                    (function(canvas, index) {
                        try {
                            // Garante que o canvas tenha um ID
                            if (!canvas.id) {
                                canvas.id = 'canvas_' + index + '_' + Date.now();
                            }
                            
                            // Tenta capturar o canvas
                            setTimeout(function() {
                                try {
                                    var dataURL = canvas.toDataURL('image/png');
                                    graficos[canvas.id] = {
                                        dataURL: dataURL,
                                        width: canvas.width,
                                        height: canvas.height,
                                        style: canvas.getAttribute('style') || ''
                                    };
                                } catch (e) {
                                    console.warn('Erro ao capturar canvas:', e);
                                }
                                
                                processados++;
                                if (processados === canvasElements.length) {
                                    resolve(graficos);
                                }
                            }, 100);
                            
                        } catch (e) {
                            console.warn('Erro ao processar canvas:', e);
                            processados++;
                            if (processados === canvasElements.length) {
                                resolve(graficos);
                            }
                        }
                    })(canvasElements[i], i);
                }
                
                // Timeout de segurança
                setTimeout(function() {
                    resolve(graficos);
                }, 2000);
                
            } catch (e) {
                console.error('Erro geral ao capturar gráficos:', e);
                resolve({});
            }
        });
    }
    
    // Limpa o HTML
    function limparHTML(htmlString, graficos) {
        try {
            // Cria um parser DOM
            var parser = new DOMParser();
            var doc = parser.parseFromString(htmlString, 'text/html');
            
            // Substitui canvas por imagens
            if (graficos && Object.keys(graficos).length > 0) {
                for (var id in graficos) {
                    var canvas = doc.getElementById(id);
                    if (canvas && graficos[id]) {
                        var img = doc.createElement('img');
                        img.src = graficos[id].dataURL;
                        img.style.maxWidth = '100%';
                        img.style.height = 'auto';
                        img.className = 'grafico-exportado';
                        if (graficos[id].style) {
                            img.setAttribute('style', graficos[id].style + '; max-width: 100%; height: auto;');
                        }
                        canvas.parentNode.replaceChild(img, canvas);
                    }
                }
            }
            
            // Remove elementos indesejados
            var seletores = [
                'script', 'button', '.btn', 'select', 'input', 'textarea',
                '.action-bar', '.action-buttons', '.filter-section',
                '.modal', '.loading', '.spinner', '.no-print', '.no-export'
            ];
            
            seletores.forEach(function(seletor) {
                var elementos = doc.querySelectorAll(seletor);
                for (var i = elementos.length - 1; i >= 0; i--) {
                    if (elementos[i].parentNode) {
                        elementos[i].parentNode.removeChild(elementos[i]);
                    }
                }
            });
            
            // Remove atributos de eventos
            var todosElementos = doc.querySelectorAll('*');
            for (var i = 0; i < todosElementos.length; i++) {
                var el = todosElementos[i];
                var attrs = el.attributes;
                for (var j = attrs.length - 1; j >= 0; j--) {
                    if (attrs[j].name.indexOf('on') === 0) {
                        el.removeAttribute(attrs[j].name);
                    }
                }
            }
            
            // Converte links em texto
            var links = doc.querySelectorAll('a');
            for (var i = 0; i < links.length; i++) {
                var link = links[i];
                if (link.className && (link.className.indexOf('btn') >= 0 || link.className.indexOf('button') >= 0)) {
                    if (link.parentNode) {
                        link.parentNode.removeChild(link);
                    }
                } else {
                    var span = doc.createElement('span');
                    span.innerHTML = link.innerHTML;
                    if (link.className) span.className = link.className;
                    if (link.parentNode) {
                        link.parentNode.replaceChild(span, link);
                    }
                }
            }
            
            return doc.documentElement.outerHTML;
            
        } catch (e) {
            console.error('Erro ao limpar HTML:', e);
            return htmlString;
        }
    }
    
    // Captura CSS da página
    function capturarCSS() {
        var css = '';
        
        try {
            // Captura stylesheets
            for (var i = 0; i < document.styleSheets.length; i++) {
                try {
                    var sheet = document.styleSheets[i];
                    if (sheet.cssRules) {
                        for (var j = 0; j < sheet.cssRules.length; j++) {
                            css += sheet.cssRules[j].cssText + '\n';
                        }
                    }
                } catch (e) {
                    // Ignora erros de CORS
                }
            }
            
            // Captura styles inline
            var styles = document.querySelectorAll('style');
            for (var i = 0; i < styles.length; i++) {
                css += styles[i].innerHTML + '\n';
            }
            
        } catch (e) {
            console.warn('Erro ao capturar CSS:', e);
        }
        
        // CSS adicional para exportação
        css += '\n/* Estilos de Exportação */\n';
        css += 'button, .btn, select, input, .action-bar, .filter-section { display: none !important; }\n';
        css += 'a { text-decoration: none !important; cursor: default !important; pointer-events: none !important; }\n';
        css += '.grafico-exportado { display: block !important; margin: 1rem auto !important; }\n';
        css += '* { animation: none !important; transition: none !important; }\n';
        
        return css;
    }
    
    // Gera o HTML completo
    function gerarHTML(titulo, conteudo, css, opcoes) {
        opcoes = opcoes || {};
        
        var dataHora = new Date().toLocaleString('pt-BR');
        
        var html = '<!DOCTYPE html>\n';
        html += '<html lang="pt-BR">\n';
        html += '<head>\n';
        html += '<meta charset="UTF-8">\n';
        html += '<meta name="viewport" content="width=device-width, initial-scale=1.0">\n';
        html += '<title>' + titulo + '</title>\n';
        html += '<style>\n';
        html += '* { margin: 0; padding: 0; box-sizing: border-box; }\n';
        html += 'body { font-family: Arial, sans-serif; line-height: 1.6; color: #333; background: white; padding: 20px; }\n';
        html += '.export-header { background: #f8f9fa; border: 1px solid #dee2e6; padding: 15px; margin-bottom: 20px; border-radius: 5px; }\n';
        html += '.export-footer { margin-top: 40px; padding-top: 20px; border-top: 2px solid #dee2e6; text-align: center; font-size: 12px; color: #6c757d; }\n';
        html += css;
        html += '</style>\n';
        html += '</head>\n';
        html += '<body>\n';
        
        if (opcoes.incluirData !== false) {
            html += '<div class="export-header">\n';
            html += '<strong>' + titulo + '</strong><br>\n';
            html += 'Documento exportado em: ' + dataHora + '\n';
            html += '</div>\n';
        }
        
        html += conteudo;
        
        if (opcoes.incluirRodape !== false) {
            html += '<div class="export-footer">\n';
            html += 'Este documento foi gerado automaticamente pelo sistema.\n';
            html += '</div>\n';
        }
        
        html += '</body>\n';
        html += '</html>';
        
        return html;
    }
    
    // Função para baixar arquivo
    function baixarArquivo(conteudo, nomeArquivo) {
        try {
            var blob = new Blob([conteudo], { type: 'text/html;charset=utf-8' });
            var url = window.URL.createObjectURL(blob);
            var link = document.createElement('a');
            
            link.href = url;
            link.download = nomeArquivo;
            link.style.display = 'none';
            
            document.body.appendChild(link);
            link.click();
            
            setTimeout(function() {
                document.body.removeChild(link);
                window.URL.revokeObjectURL(url);
            }, 100);
            
        } catch (e) {
            console.error('Erro ao baixar arquivo:', e);
            throw e;
        }
    }
    
    // Mostra feedback
    function mostrarFeedback(tipo, mensagem) {
        var div = document.createElement('div');
        div.style.cssText = 'position: fixed; top: 20px; right: 20px; padding: 15px 25px; border-radius: 8px; z-index: 10000;';
        
        if (tipo === 'sucesso') {
            div.style.background = '#28a745';
            div.style.color = 'white';
            div.innerHTML = '✓ ' + mensagem;
        } else {
            div.style.background = '#dc3545';
            div.style.color = 'white';
            div.innerHTML = '✗ ' + mensagem;
        }
        
        document.body.appendChild(div);
        
        setTimeout(function() {
            if (div.parentNode) {
                div.parentNode.removeChild(div);
            }
        }, 3000);
    }
    
    // Gera nome do arquivo
    function gerarNomeArquivo(prefixo) {
        var data = new Date();
        var ano = data.getFullYear();
        var mes = ('0' + (data.getMonth() + 1)).slice(-2);
        var dia = ('0' + data.getDate()).slice(-2);
        var hora = ('0' + data.getHours()).slice(-2);
        var minuto = ('0' + data.getMinutes()).slice(-2);
        
        var nome = prefixo + '_' + ano + mes + dia + '_' + hora + minuto;
        
        // Adiciona informações do contexto se disponíveis
        try {
            var coug = document.getElementById('seletor-coug');
            if (coug && coug.value) {
                nome += '_' + coug.value;
            }
        } catch (e) {}
        
        return nome + '.html';
    }
    
    // Função principal de download
    function executarDownload(opcoes) {
        opcoes = opcoes || {};
        
        removerLoadings();
        
        // Aguarda e captura gráficos
        setTimeout(function() {
            capturarGraficos().then(function(graficos) {
                try {
                    // Captura HTML atual
                    var htmlOriginal = document.documentElement.outerHTML;
                    
                    // Limpa HTML
                    var htmlLimpo = limparHTML(htmlOriginal, graficos);
                    
                    // Captura CSS
                    var css = capturarCSS();
                    
                    // Extrai body
                    var parser = new DOMParser();
                    var doc = parser.parseFromString(htmlLimpo, 'text/html');
                    var bodyContent = doc.body.innerHTML;
                    
                    // Gera HTML final
                    var titulo = opcoes.titulo || document.title;
                    var htmlFinal = gerarHTML(titulo, bodyContent, css, opcoes);
                    
                    // Nome do arquivo
                    var nomeArquivo = opcoes.nomeArquivo || gerarNomeArquivo(opcoes.prefixo || 'relatorio');
                    
                    // Faz download
                    baixarArquivo(htmlFinal, nomeArquivo);
                    
                    // Mostra sucesso
                    if (opcoes.mostrarFeedback !== false) {
                        mostrarFeedback('sucesso', 'Download realizado com sucesso!');
                    }
                    
                } catch (e) {
                    console.error('Erro ao processar download:', e);
                    if (opcoes.mostrarFeedback !== false) {
                        mostrarFeedback('erro', 'Erro ao gerar o download. Tente novamente.');
                    }
                }
            });
        }, 500);
    }
    
    // Exporta API global
    window.DownloadHTML = {
        baixarRelatorio: executarDownload,
        
        baixarBalancoOrcamentario: function() {
            executarDownload({
                prefixo: 'balanco_orcamentario_receita',
                titulo: 'Balanço Orçamentário da Receita',
                incluirData: true,
                incluirRodape: true
            });
        },
        
        baixarRelatorioInconsistencias: function() {
            executarDownload({
                prefixo: 'analise_inconsistencias',
                titulo: 'Análise de Inconsistências',
                incluirData: true,
                incluirRodape: true
            });
        }
    };
    
    // Compatibilidade
    window.baixarRelatorioHTML = executarDownload;
    
})();