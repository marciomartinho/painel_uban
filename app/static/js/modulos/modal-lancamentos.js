// app/static/js/modulos/modal-lancamentos.js
/**
 * Módulo JavaScript para gerenciar modais de lançamentos
 * Fornece funcionalidade reutilizável para exibir lançamentos em qualquer relatório
 */

window.ModalLancamentos = (function() {
    'use strict';
    
    // Configurações padrão
    const config = {
        apiUrl: '/relatorios/api/modal-lancamentos',
        modalId: 'modal-lancamentos-global',
        overlayClass: 'modal-lancamentos-overlay',
        contentClass: 'modal-lancamentos-content',
        loadingText: 'Carregando lançamentos...',
        erroText: 'Erro ao carregar lançamentos',
        fecharComEsc: true,
        fecharAoClicarFora: true,
        animacao: true
    };
    
    // Estado do modal
    let modalAtual = null;
    let callbackFechar = null;
    
    /**
     * Cria estrutura HTML do modal se não existir
     */
    function criarModalHTML() {
        if (document.getElementById(config.modalId)) {
            return; // Modal já existe
        }
        
        const modalHTML = `
            <div id="${config.modalId}" class="${config.overlayClass}" style="display: none;">
                <div class="${config.contentClass}">
                    <div class="modal-lancamentos-header">
                        <div id="modal-lancamentos-titulo"></div>
                        <button class="modal-lancamentos-fechar" onclick="ModalLancamentos.fechar()">&times;</button>
                    </div>
                    <div id="modal-lancamentos-body" class="modal-lancamentos-body">
                        <div class="modal-lancamentos-loading">
                            <i class="fas fa-spinner fa-spin"></i> ${config.loadingText}
                        </div>
                    </div>
                </div>
            </div>
        `;
        
        document.body.insertAdjacentHTML('beforeend', modalHTML);
        
        // Adiciona event listeners
        configurarEventListeners();
    }
    
    /**
     * Configura event listeners do modal
     */
    function configurarEventListeners() {
        const modal = document.getElementById(config.modalId);
        
        // Fechar ao clicar fora
        if (config.fecharAoClicarFora) {
            modal.addEventListener('click', function(e) {
                if (e.target === modal) {
                    fechar();
                }
            });
        }
        
        // Fechar com ESC
        if (config.fecharComEsc) {
            document.addEventListener('keydown', function(e) {
                if (e.key === 'Escape' && modalAtual) {
                    fechar();
                }
            });
        }
    }
    
    /**
     * Abre o modal de lançamentos
     * @param {HTMLElement|Object} elementoOuParams - Elemento do botão ou objeto com parâmetros
     * @param {Object} opcoes - Opções adicionais para o modal
     */
    async function abrir(elementoOuParams, opcoes = {}) {
        // Cria modal se não existir
        criarModalHTML();
        
        let params = {};
        let elemento = null;
        
        // Determina se é elemento ou parâmetros diretos
        if (elementoOuParams instanceof HTMLElement) {
            elemento = elementoOuParams;
            params = JSON.parse(elemento.dataset.params || '{}');
        } else {
            params = elementoOuParams;
        }
        
        // Merge com opções
        const configuracao = { ...config, ...opcoes };
        
        // Obtém informações do contexto se houver elemento
        let contexto = {};
        if (elemento) {
            const linha = elemento.closest('tr');
            if (linha) {
                const descricao = linha.querySelector('.item-description, td:first-child')?.textContent;
                const valorCelula = linha.cells[3]?.textContent; // Ajustar índice conforme necessário
                
                contexto = {
                    descricao: descricao?.trim(),
                    valorRelatorio: valorCelula?.trim()
                };
            }
        }
        
        // Mostra modal com loading
        mostrarModal();
        
        try {
            // Busca dados
            const response = await fetch(configuracao.apiUrl, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    ...params,
                    formato: 'html',
                    colunas: configuracao.colunas
                })
            });
            
            const data = await response.json();
            
            if (!response.ok || data.erro) {
                throw new Error(data.erro || 'Erro ao buscar dados');
            }
            
            // Monta título
            const titulo = montarTitulo(contexto, params, data);
            document.getElementById('modal-lancamentos-titulo').innerHTML = titulo;
            
            // Exibe dados
            const body = document.getElementById('modal-lancamentos-body');
            
            if (data.quantidade === 0) {
                body.innerHTML = `
                    <div class="modal-lancamentos-vazio">
                        <i class="fas fa-inbox fa-3x"></i>
                        <p>Nenhum lançamento encontrado para os filtros aplicados.</p>
                    </div>
                `;
            } else {
                body.innerHTML = data.html;
                
                // Adiciona informações adicionais se configurado
                if (configuracao.mostrarInfo) {
                    adicionarInfoAdicional(body, data, contexto);
                }
            }
            
            // Callback de sucesso
            if (configuracao.onSuccess) {
                configuracao.onSuccess(data);
            }
            
        } catch (error) {
            console.error('Erro ao buscar lançamentos:', error);
            
            const body = document.getElementById('modal-lancamentos-body');
            body.innerHTML = `
                <div class="modal-lancamentos-erro">
                    <i class="fas fa-exclamation-triangle fa-3x"></i>
                    <p>${configuracao.erroText}</p>
                    <small>${error.message}</small>
                </div>
            `;
            
            // Callback de erro
            if (configuracao.onError) {
                configuracao.onError(error);
            }
        }
    }
    
    /**
     * Monta título do modal baseado no contexto
     */
    function montarTitulo(contexto, params, data) {
        let html = '<h3>';
        
        // Título da unidade gestora se houver
        if (params.coug) {
            const seletorCoug = document.getElementById('seletor-coug');
            if (seletorCoug) {
                const cougTexto = seletorCoug.options[seletorCoug.selectedIndex]?.text || '';
                html += `<div class="modal-lancamentos-ug">${cougTexto.replace(/[📊🏛️]/g, '').trim()}</div>`;
            }
        }
        
        // Descrição do item
        if (contexto.descricao) {
            html += `Lançamentos para: ${contexto.descricao}`;
        } else {
            html += 'Lançamentos Detalhados';
        }
        
        html += '</h3>';
        
        // Valor do relatório vs valor dos lançamentos
        if (contexto.valorRelatorio) {
            html += `
                <div class="modal-lancamentos-comparacao">
                    <span>Valor no Relatório: <strong>${contexto.valorRelatorio}</strong></span>
                    ${data.totais ? `<span>Total dos Lançamentos: <strong class="${data.totais.total_liquido >= 0 ? 'valor-positivo' : 'valor-negativo'}">${formatarMoeda(data.totais.total_liquido)}</strong></span>` : ''}
                </div>
            `;
        }
        
        return html;
    }
    
    /**
     * Adiciona informações adicionais ao corpo do modal
     */
    function adicionarInfoAdicional(body, data, contexto) {
        const info = document.createElement('div');
        info.className = 'modal-lancamentos-info-adicional';
        
        let html = '<h4>Informações Adicionais</h4><ul>';
        
        if (data.totais) {
            html += `<li>Total de Débitos: ${formatarMoeda(data.totais.total_debito)}</li>`;
            html += `<li>Total de Créditos: ${formatarMoeda(data.totais.total_credito)}</li>`;
            html += `<li>Diferença (C-D): ${formatarMoeda(data.totais.total_liquido)}</li>`;
        }
        
        html += '</ul>';
        info.innerHTML = html;
        
        body.appendChild(info);
    }
    
    /**
     * Mostra o modal com animação
     */
    function mostrarModal() {
        const modal = document.getElementById(config.modalId);
        modal.style.display = 'flex';
        
        if (config.animacao) {
            modal.classList.add('modal-lancamentos-animacao-entrada');
            setTimeout(() => {
                modal.classList.remove('modal-lancamentos-animacao-entrada');
            }, 300);
        }
        
        modalAtual = modal;
        document.body.style.overflow = 'hidden'; // Previne scroll do body
    }
    
    /**
     * Fecha o modal
     */
    function fechar() {
        const modal = document.getElementById(config.modalId);
        
        if (config.animacao) {
            modal.classList.add('modal-lancamentos-animacao-saida');
            setTimeout(() => {
                modal.style.display = 'none';
                modal.classList.remove('modal-lancamentos-animacao-saida');
                limparModal();
            }, 300);
        } else {
            modal.style.display = 'none';
            limparModal();
        }
        
        document.body.style.overflow = ''; // Restaura scroll do body
        modalAtual = null;
        
        // Executa callback se houver
        if (callbackFechar) {
            callbackFechar();
            callbackFechar = null;
        }
    }
    
    /**
     * Limpa conteúdo do modal
     */
    function limparModal() {
        document.getElementById('modal-lancamentos-titulo').innerHTML = '';
        document.getElementById('modal-lancamentos-body').innerHTML = `
            <div class="modal-lancamentos-loading">
                <i class="fas fa-spinner fa-spin"></i> ${config.loadingText}
            </div>
        `;
    }
    
    /**
     * Formata valor monetário
     */
    function formatarMoeda(valor) {
        return new Intl.NumberFormat('pt-BR', {
            style: 'currency',
            currency: 'BRL'
        }).format(valor || 0);
    }
    
    /**
     * Configura opções globais do modal
     */
    function configurar(opcoes) {
        Object.assign(config, opcoes);
    }
    
    /**
     * Cria atalho para relatório específico
     */
    function criarAtalho(nomeRelatorio, opcoesEspecificas) {
        return function(elemento) {
            const opcoesCombinadas = {
                ...opcoesEspecificas,
                relatorio: nomeRelatorio
            };
            return abrir(elemento, opcoesCombinadas);
        };
    }
    
    // API pública
    return {
        abrir: abrir,
        fechar: fechar,
        configurar: configurar,
        criarAtalho: criarAtalho,
        
        // Atalhos pré-configurados
        abrirReceitaDetalhada: criarAtalho('receita_detalhada', {
            colunas: ['COCONTACONTABIL', 'COUG', 'NUDOCUMENTO', 'COEVENTO', 'INDEBITOCREDITO', 'VALANCAMENTO'],
            mostrarInfo: true
        }),
        
        abrirDespesaDetalhada: criarAtalho('despesa_detalhada', {
            colunas: ['COCONTACONTABIL', 'COUG', 'NUDOCUMENTO', 'COEVENTO', 'INDEBITOCREDITO', 'VALANCAMENTO', 'NOCREDORDEBITOR'],
            mostrarInfo: true
        })
    };
})();

// Inicializa quando o DOM estiver pronto
document.addEventListener('DOMContentLoaded', function() {
    // Configurações globais podem ser aplicadas aqui se necessário
    // ModalLancamentos.configurar({ ... });
});