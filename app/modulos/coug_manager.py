# app/modulos/coug_manager.py
"""
Módulo de Gerenciamento de COUG (Código da Unidade Gestora)
Centraliza todas as funcionalidades relacionadas a seleção e filtro de COUGs
"""
from flask import request
from app.modulos.conexao_hibrida import adaptar_query # Importa o adaptador

class COUGManager:
    """Gerencia seleção e filtros de COUG em todo o sistema"""
    
    def __init__(self, conn):
        self.conn = conn
        self._cache_cougs = None
        
    def listar_cougs_com_movimento(self, filtros_conta: list = None) -> list:
        if self._cache_cougs is not None and not filtros_conta:
            return self._cache_cougs
            
        condicao_filtros = ""
        if filtros_conta:
            condicao_filtros = f"AND fs.saldo_contabil != 0 AND ({' OR '.join(filtros_conta)})"
        
        query_original = f"""
        SELECT DISTINCT 
            fs.COUG as codigo,
            COALESCE(ug.NOUG, 'UG ' || fs.COUG) as nome,
            fs.COUG || ' - ' || COALESCE(ug.NOUG, 'UG ' || fs.COUG) as descricao_completa
        FROM fato_saldos fs
        LEFT JOIN dimensoes.unidades_gestoras ug ON fs.COUG = ug.COUG
        WHERE fs.COUG IS NOT NULL AND fs.COUG != ''
            {condicao_filtros}
        ORDER BY fs.COUG
        """
        query_adaptada = adaptar_query(query_original)
        
        try:
            cursor = self.conn.cursor()
            # No PostgreSQL, a ordenação de texto pode não funcionar como inteiro.
            # A ordenação por COUG como texto geralmente é suficiente.
            cursor.execute(query_adaptada)
            
            cougs = [{'codigo': row['codigo'], 'nome': row['nome'], 'descricao_completa': row['descricao_completa']} for row in cursor.fetchall()]
            
            if not filtros_conta:
                self._cache_cougs = cougs
                
            return cougs
            
        except Exception as e:
            print(f"Erro ao buscar COUGs: {e}")
            return []
    
    def get_nome_coug(self, coug: str) -> str:
        if not coug: return "CONSOLIDADO"
            
        query_original = """
        SELECT COALESCE(ug.NOUG, 'UG ' || %s) as nome
        FROM (SELECT %s as coug) c
        LEFT JOIN dimensoes.unidades_gestoras ug ON c.coug = ug.COUG
        """
        query_adaptada = adaptar_query(query_original).replace('%s', '?') # Adapta para SQLite
        
        try:
            cursor = self.conn.cursor()
            cursor.execute(query_adaptada, (coug, coug))
            resultado = cursor.fetchone()
            return resultado['nome'] if resultado else f"UG {coug}"
        except Exception as e:
            print(f"Erro em get_nome_coug: {e}")
            return f"UG {coug}"

    def get_coug_da_url(self) -> str | None:
        return request.args.get('coug', '')
        
    def aplicar_filtro_query(self, alias_tabela: str = "fs", coug: str = None) -> str:
        if not coug: return ""
        # Usar placeholders é mais seguro, mas para essa lógica simples, a concatenação é aceitável.
        coug_escaped = str(coug).replace("'", "''")
        return f" AND {alias_tabela}.COUG = '{coug_escaped}'"

    def get_sufixo_arquivo(self, coug_selecionada: str = '') -> str:
        return '_consolidado' if not coug_selecionada else f'_coug_{coug_selecionada}'