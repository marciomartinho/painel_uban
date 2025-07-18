# app/modulos/coug_manager.py
"""
Módulo de Gerenciamento de COUG (Código da Unidade Gestora)
Centraliza todas as funcionalidades relacionadas a seleção e filtro de COUGs
"""
from flask import request
from .conexao_hibrida import adaptar_query, get_db_environment

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
        
        # --- CORREÇÃO AQUI ---
        type_cast = "::text" if get_db_environment() == 'postgres' else ""

        query_original = f"""
        SELECT DISTINCT
            fs.coug as codigo,
            COALESCE(ug.noug, 'UG ' || fs.coug) as nome,
            fs.coug || ' - ' || COALESCE(ug.noug, 'UG ' || fs.coug) as descricao_completa
        FROM fato_saldos fs
        LEFT JOIN dimensoes.unidades_gestoras ug ON fs.coug{type_cast} = ug.coug
        WHERE fs.coug IS NOT NULL
            {condicao_filtros}
        ORDER BY fs.coug
        """
        query_adaptada = adaptar_query(query_original)

        try:
            # Acessa o cursor a partir da conexão
            cursor = self.conn.cursor()
            cursor.execute(query_adaptada)

            # Para compatibilidade, extrai nomes das colunas e cria dicts
            colunas = [desc[0].lower() for desc in cursor.description]
            cougs = [dict(zip(colunas, row)) for row in cursor.fetchall()]

            if not filtros_conta:
                self._cache_cougs = cougs

            return cougs

        except Exception as e:
            print(f"Erro ao buscar COUGs: {e}")
            return []

    def get_nome_coug(self, coug: str) -> str:
        if not coug: return "CONSOLIDADO"
        
        type_cast = "::text" if get_db_environment() == 'postgres' else ""

        query_original = f"""
        SELECT COALESCE(ug.noug, 'UG ' || %s) as nome
        FROM (SELECT %s as coug) c
        LEFT JOIN dimensoes.unidades_gestoras ug ON c.coug{type_cast} = ug.coug
        """
        query_adaptada = adaptar_query(query_original)

        try:
            cursor = self.conn.cursor()
            cursor.execute(query_adaptada, (coug, coug))
            resultado = cursor.fetchone()
            return resultado[0] if resultado else f"UG {coug}"
        except Exception as e:
            print(f"Erro em get_nome_coug: {e}")
            return f"UG {coug}"

    def get_coug_da_url(self) -> str | None:
        return request.args.get('coug', '')

    def aplicar_filtro_query(self, alias_tabela: str = "fs", coug: str = None) -> str:
        if not coug: return ""
        coug_escaped = str(coug).replace("'", "''")
        return f" AND {alias_tabela}.coug = '{coug_escaped}'"

    def get_sufixo_arquivo(self, coug_selecionada: str = '') -> str:
        return '_consolidado' if not coug_selecionada else f'_coug_{coug_selecionada}'