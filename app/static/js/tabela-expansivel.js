// static/js/componentes/tabela-expansivel.js

class TabelaExpansivel {
    constructor(containerId, opcoes = {}) {
        this.container = document.getElementById(containerId);
        this.opcoes = {
            salvarEstado: true,
            animacao: true,
            expandirTodos: false,
            recolherTodos: false,
            indentacaoPorNivel: 20,
            ...opcoes
        };
        
        this.estadoAberto = {};
        this.chaveStorage = `tabela-expansivel-${containerId}`;
        
        this.inicializar();
    }
    
    inicializar() {
        if (!this.container) return;
        
        // Carrega estado salvo
        if (this.opcoes.salvarEstado) {
            this.carregarEstado();
        }
        
        // Adiciona botões de expandir/recolher
        this.adicionarBotoes();
        
        // Adiciona controles gerais se configurado
        if (this.opcoes.expandirTodos || this.opcoes.recolherTodos) {
            this.adicionarControlesGerais();
        }
        
        // Aplica estado inicial
        this.aplicarEstadoInicial();
        
        // Configura eventos
        this.configurarEventos();
    }
    
    adicionarBotoes() {
        const linhasExpandiveis = this.container.querySelectorAll('tr[data-nivel]');
        
        linhasExpandiveis.forEach(linha => {
            const nivel = parseInt(linha.dataset.nivel);
            const id = linha.dataset.id;
            const temFilhos = this.temFilhos(id);
            
            if (temFilhos) {
                const primeiraCelula = linha.querySelector('td:first-child');
                const conteudoAtual = primeiraCelula.innerHTML;
                
                // Adiciona indentação baseada no nível
                const indentacao = nivel * this.opcoes.indentacaoPorNivel;
                
                primeiraCelula.innerHTML = `
                    <span class="expansivel-container" style="padding-left: ${indentacao}px;">
                        <button class="btn-expandir" data-id="${id}" aria-expanded="false">
                            <span class="icone">▶</span>
                        </button>
                        <span class="conteudo-celula">${conteudoAtual}</span>
                    </span>
                `;
            } else {
                // Apenas adiciona indentação para itens sem filhos
                const primeiraCelula = linha.querySelector('td:first-child');
                const indentacao = (nivel * this.opcoes.indentacaoPorNivel) + 24; // 24px para alinhar com items que tem botão
                primeiraCelula.style.paddingLeft = `${indentacao}px`;
            }
        });
    }
    
    temFilhos(idPai) {
        return this.container.querySelector(`tr[data-pai="${idPai}"]`) !== null;
    }
    
    adicionarControlesGerais() {
        const controlesHtml = `
            <div class="controles-expansao">
                ${this.opcoes.expandirTodos ? '<button id="expandir-todos" class="btn btn-sm">Expandir Todos</button>' : ''}
                ${this.opcoes.recolherTodos ? '<button id="recolher-todos" class="btn btn-sm">Recolher Todos</button>' : ''}
            </div>
        `;
        
        this.container.insertAdjacentHTML('beforebegin', controlesHtml);
    }
    
    configurarEventos() {
        // Eventos dos botões de expandir/recolher individual
        this.container.addEventListener('click', (e) => {
            const btn = e.target.closest('.btn-expandir');
            if (btn) {
                e.preventDefault();
                const id = btn.dataset.id;
                this.alternarLinha(id, btn);
            }
        });
        
        // Eventos dos controles gerais
        if (this.opcoes.expandirTodos) {
            document.getElementById('expandir-todos')?.addEventListener('click', () => {
                this.expandirTodos();
            });
        }
        
        if (this.opcoes.recolherTodos) {
            document.getElementById('recolher-todos')?.addEventListener('click', () => {
                this.recolherTodos();
            });
        }
        
        // Evento de tecla para acessibilidade
        this.container.addEventListener('keydown', (e) => {
            if (e.key === 'Enter' || e.key === ' ') {
                const btn = e.target.closest('.btn-expandir');
                if (btn) {
                    e.preventDefault();
                    const id = btn.dataset.id;
                    this.alternarLinha(id, btn);
                }
            }
        });
    }
    
    alternarLinha(id, botao) {
        const estaAberto = this.estadoAberto[id] || false;
        
        if (estaAberto) {
            this.recolher(id, botao);
        } else {
            this.expandir(id, botao);
        }
        
        // Salva estado
        if (this.opcoes.salvarEstado) {
            this.salvarEstado();
        }
    }
    
    expandir(id, botao) {
        const filhos = this.container.querySelectorAll(`tr[data-pai="${id}"]`);
        
        filhos.forEach(filho => {
            if (this.opcoes.animacao) {
                filho.style.display = 'table-row';
                filho.classList.add('expandindo');
                setTimeout(() => filho.classList.remove('expandindo'), 300);
            } else {
                filho.style.display = 'table-row';
            }
            
            // Se o filho também estava expandido, mostra seus filhos
            const idFilho = filho.dataset.id;
            if (this.estadoAberto[idFilho]) {
                this.expandir(idFilho, filho.querySelector('.btn-expandir'));
            }
        });
        
        // Atualiza botão
        if (botao) {
            botao.querySelector('.icone').textContent = '▼';
            botao.setAttribute('aria-expanded', 'true');
        }
        
        this.estadoAberto[id] = true;
    }
    
    recolher(id, botao) {
        const filhos = this.obterTodosDescendentes(id);
        
        filhos.forEach(filho => {
            if (this.opcoes.animacao) {
                filho.classList.add('recolhendo');
                setTimeout(() => {
                    filho.style.display = 'none';
                    filho.classList.remove('recolhendo');
                }, 300);
            } else {
                filho.style.display = 'none';
            }
        });
        
        // Atualiza botão
        if (botao) {
            botao.querySelector('.icone').textContent = '▶';
            botao.setAttribute('aria-expanded', 'false');
        }
        
        this.estadoAberto[id] = false;
    }
    
    obterTodosDescendentes(id) {
        const descendentes = [];
        const filhosDiretos = this.container.querySelectorAll(`tr[data-pai="${id}"]`);
        
        filhosDiretos.forEach(filho => {
            descendentes.push(filho);
            const idFilho = filho.dataset.id;
            descendentes.push(...this.obterTodosDescendentes(idFilho));
        });
        
        return descendentes;
    }
    
    expandirTodos() {
        const botoes = this.container.querySelectorAll('.btn-expandir');
        botoes.forEach(btn => {
            const id = btn.dataset.id;
            if (!this.estadoAberto[id]) {
                this.expandir(id, btn);
            }
        });
        
        if (this.opcoes.salvarEstado) {
            this.salvarEstado();
        }
    }
    
    recolherTodos() {
        const botoes = this.container.querySelectorAll('.btn-expandir');
        botoes.forEach(btn => {
            const id = btn.dataset.id;
            if (this.estadoAberto[id]) {
                this.recolher(id, btn);
            }
        });
        
        if (this.opcoes.salvarEstado) {
            this.salvarEstado();
        }
    }
    
    aplicarEstadoInicial() {
        // Primeiro, esconde todos os filhos
        const todasLinhas = this.container.querySelectorAll('tr[data-pai]');
        todasLinhas.forEach(linha => {
            linha.style.display = 'none';
        });
        
        // Depois, expande os que estavam abertos
        Object.keys(this.estadoAberto).forEach(id => {
            if (this.estadoAberto[id]) {
                const botao = this.container.querySelector(`.btn-expandir[data-id="${id}"]`);
                if (botao) {
                    this.expandir(id, botao);
                }
            }
        });
    }
    
    salvarEstado() {
        localStorage.setItem(this.chaveStorage, JSON.stringify(this.estadoAberto));
    }
    
    carregarEstado() {
        const estadoSalvo = localStorage.getItem(this.chaveStorage);
        if (estadoSalvo) {
            this.estadoAberto = JSON.parse(estadoSalvo);
        }
    }
    
    // Método para resetar o estado
    resetarEstado() {
        this.estadoAberto = {};
        localStorage.removeItem(this.chaveStorage);
        this.recolherTodos();
    }
    
    // Método para obter dados para impressão (tudo expandido)
    prepararParaImpressao() {
        const estadoAtual = {...this.estadoAberto};
        this.expandirTodos();
        
        // Retorna função para restaurar estado
        return () => {
            this.estadoAberto = estadoAtual;
            this.aplicarEstadoInicial();
        };
    }
}

// Função helper para inicialização fácil
function inicializarTabelaExpansivel(containerId, opcoes = {}) {
    return new TabelaExpansivel(containerId, opcoes);
}

// Exporta para uso global
window.TabelaExpansivel = TabelaExpansivel;
window.inicializarTabelaExpansivel = inicializarTabelaExpansivel;