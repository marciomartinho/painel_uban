# app/modulos/coug_manager.py
"""
Módulo de Gerenciamento de COUG (Código da Unidade Gestora)
Centraliza todas as funcionalidades relacionadas a seleção e filtro de COUGs
"""

import sqlite3
from typing import List, Dict, Optional, Tuple
from flask import request

class COUGManager:
    """Gerencia seleção e filtros de COUG em todo o sistema"""
    
    def __init__(self, conn: sqlite3.Connection):
        """
        Inicializa o gerenciador com uma conexão de banco
        
        Args:
            conn: Conexão SQLite com banco principal e dimensões anexadas
        """
        self.conn = conn
        self._cache_cougs = None
        
    def listar_cougs_com_movimento(self, filtros_conta: List[str] = None) -> List[Dict[str, str]]:
        """
        Lista COUGs que possuem movimento nas contas especificadas
        
        Args:
            filtros_conta: Lista de filtros SQL de conta (ex: resultado de get_filtro_conta())
                          Se None, retorna todas as COUGs
        
        Returns:
            Lista de dicts com 'codigo', 'nome' e 'descricao_completa'
        """
        if self._cache_cougs is not None and not filtros_conta:
            return self._cache_cougs
            
        # Monta condição de filtros
        condicao_filtros = ""
        if filtros_conta:
            condicao_filtros = f"""
                AND fs.saldo_contabil != 0
                AND ({' OR '.join(filtros_conta)})
            """
        
        query = f"""
        SELECT DISTINCT 
            fs.COUG as codigo,
            COALESCE(ug.NOUG, 'UG ' || fs.COUG) as nome,
            fs.COUG || ' - ' || COALESCE(ug.NOUG, 'UG ' || fs.COUG) as descricao_completa
        FROM fato_saldos fs
        LEFT JOIN dimensoes.unidades_gestoras ug ON fs.COUG = ug.COUG
        WHERE fs.COUG IS NOT NULL 
            AND fs.COUG != ''
            {condicao_filtros}
        ORDER BY CAST(fs.COUG AS INTEGER)
        """
        
        try:
            cursor = self.conn.execute(query)
            cougs = []
            
            for row in cursor:
                cougs.append({
                    'codigo': row['codigo'],
                    'nome': row['nome'],
                    'descricao_completa': row['descricao_completa']
                })
            
            # Cache se não houver filtros
            if not filtros_conta:
                self._cache_cougs = cougs
                
            return cougs
            
        except Exception as e:
            print(f"Erro ao buscar COUGs: {e}")
            return []
    
    def validar_coug(self, coug: str) -> bool:
        """
        Verifica se uma COUG existe no banco
        
        Args:
            coug: Código da COUG
            
        Returns:
            True se existe, False caso contrário
        """
        if not coug:
            return True  # Vazio = consolidado = válido
            
        query = """
        SELECT COUNT(*) as total
        FROM fato_saldos
        WHERE COUG = ?
        LIMIT 1
        """
        
        try:
            cursor = self.conn.execute(query, (coug,))
            resultado = cursor.fetchone()
            return resultado['total'] > 0
        except:
            return False
    
    def get_nome_coug(self, coug: str) -> str:
        """
        Retorna o nome de uma COUG específica
        
        Args:
            coug: Código da COUG
            
        Returns:
            Nome da COUG ou código se não encontrar
        """
        if not coug:
            return "CONSOLIDADO"
            
        query = """
        SELECT COALESCE(ug.NOUG, 'UG ' || ?) as nome
        FROM (SELECT ? as coug) c
        LEFT JOIN dimensoes.unidades_gestoras ug ON c.coug = ug.COUG
        """
        
        try:
            cursor = self.conn.execute(query, (coug, coug))
            resultado = cursor.fetchone()
            return resultado['nome'] if resultado else f"UG {coug}"
        except:
            return f"UG {coug}"
    
    def aplicar_filtro_query(self, alias_tabela: str = "fs", coug: str = None) -> str:
        """
        Retorna fragmento SQL para filtrar por COUG
        
        Args:
            alias_tabela: Alias da tabela na query (default: fs)
            coug: Código da COUG para filtrar
            
        Returns:
            String com AND ... ou string vazia
        """
        if not coug:
            return ""
            
        # Escapa aspas simples para evitar SQL injection
        coug_escaped = coug.replace("'", "''")
        return f"AND {alias_tabela}.COUG = '{coug_escaped}'"
    
    def get_coug_da_url(self) -> Optional[str]:
        """
        Obtém COUG selecionada dos parâmetros da URL
        
        Returns:
            Código da COUG ou None
        """
        return request.args.get('coug', '')
    
    def gerar_dropdown_html(self, cougs: List[Dict], coug_selecionada: str = '') -> str:
        """
        Gera HTML do dropdown de seleção de COUG
        
        Args:
            cougs: Lista de COUGs disponíveis
            coug_selecionada: Código da COUG atualmente selecionada
            
        Returns:
            HTML do select
        """
        options = ['<option value="">📊 DADOS CONSOLIDADOS</option>']
        
        for coug in cougs:
            selected = 'selected' if coug['codigo'] == coug_selecionada else ''
            options.append(
                f'<option value="{coug["codigo"]}" {selected}>'
                f'🏛️ {coug["descricao_completa"]}'
                f'</option>'
            )
        
        return f'''
        <select id="seletor-coug" class="select-coug" onchange="mudarCOUG(this.value)">
            {" ".join(options)}
        </select>
        '''
    
    def get_titulo_relatorio(self, coug_selecionada: str = '') -> str:
        """
        Retorna título apropriado para o relatório baseado na COUG
        
        Args:
            coug_selecionada: Código da COUG
            
        Returns:
            String com título apropriado
        """
        if not coug_selecionada:
            return "Dados Consolidados - Todas as Unidades Gestoras"
        
        nome = self.get_nome_coug(coug_selecionada)
        return f"Dados da {nome}"
    
    def get_sufixo_arquivo(self, coug_selecionada: str = '') -> str:
        """
        Retorna sufixo apropriado para nome de arquivo
        
        Args:
            coug_selecionada: Código da COUG
            
        Returns:
            String com sufixo (ex: '_consolidado' ou '_coug_10101')
        """
        if not coug_selecionada:
            return '_consolidado'
        
        return f'_coug_{coug_selecionada}'

# Funções auxiliares para retrocompatibilidade e facilidade de uso

def criar_manager(conn: sqlite3.Connection) -> COUGManager:
    """Cria uma instância do COUGManager"""
    return COUGManager(conn)

def get_filtro_coug_sql(coug: str, alias_tabela: str = "fs") -> str:
    """
    Função auxiliar para gerar filtro SQL de COUG
    
    Args:
        coug: Código da COUG
        alias_tabela: Alias da tabela
        
    Returns:
        String com AND ... ou vazia
    """
    if not coug:
        return ""
    
    coug_escaped = coug.replace("'", "''")
    return f"AND {alias_tabela}.COUG = '{coug_escaped}'"

def validar_e_sanitizar_coug(coug: str) -> str:
    """
    Valida e sanitiza código de COUG
    
    Args:
        coug: Código da COUG
        
    Returns:
        COUG sanitizada ou string vazia
    """
    if not coug:
        return ""
    
    # Remove caracteres não numéricos
    import re
    coug_limpa = re.sub(r'[^0-9]', '', str(coug))
    
    return coug_limpa

# Constantes para facilitar importação
FILTRO_COUG_RECEITA = [
    "fs.COCONTACONTABIL BETWEEN '521110000' AND '521129999'",  # Previsão Inicial Líquida
    "fs.COCONTACONTABIL BETWEEN '521110000' AND '521299999'",  # Previsão Atualizada Líquida
    "fs.COCONTACONTABIL BETWEEN '621200000' AND '621399999'"   # Receita Líquida
]