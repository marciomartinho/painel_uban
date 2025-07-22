# app/relatorios/calculo_superavit_deficit.py
"""
Módulo para calcular superávit/déficit baseado nas tabelas de receitas e despesas.
Separado para manter a responsabilidade única e não interferir nos códigos existentes.
"""
import pandas as pd
from ..modulos.conexao_hibrida import ConexaoBanco, adaptar_query, get_db_environment

class CalculoSuperavitDeficit:
    """
    Calcula superávit/déficit comparando receitas realizadas com despesas empenhadas.
    """

    def __init__(self, ano: int, bimestre: int):
        self.ano = ano
        self.bimestre = bimestre
        self.bimestre_map = {
            1: [1, 2], 2: [3, 4], 3: [5, 6],
            4: [7, 8], 5: [9, 10], 6: [11, 12]
        }
        
        self.meses_ate_bimestre = []
        for i in range(1, self.bimestre + 1):
            self.meses_ate_bimestre.extend(self.bimestre_map.get(i, []))

    def _executar_query_receitas(self, query: str, params=None) -> pd.DataFrame:
        """Executa query na base de receitas."""
        with ConexaoBanco() as conn:
            query_adaptada = adaptar_query(query)
            df = pd.read_sql_query(query_adaptada, conn, params=params)
            df.columns = [col.lower() for col in df.columns]
            return df

    def _executar_query_despesas(self, query: str, params=None) -> pd.DataFrame:
        """Executa query na base de despesas."""
        with ConexaoBanco(db_name='saldos_despesa') as conn:
            query_adaptada = adaptar_query(query)
            df = pd.read_sql_query(query_adaptada, conn, params=params)
            df.columns = [col.lower() for col in df.columns]
            return df

    def _get_receitas_realizadas(self) -> float:
        """Busca total de receitas realizadas até o bimestre."""
        env = get_db_environment()
        placeholder = '%s' if env == 'postgres' else '?'
        inmes_column = "CAST(fs.inmes AS INTEGER)" if env == 'postgres' else "fs.inmes"
        coexercicio_column = "CAST(fs.coexercicio AS INTEGER)" if env == 'postgres' else "fs.coexercicio"
        
        placeholders_ate_bimestre = ', '.join([placeholder] * len(self.meses_ate_bimestre))
        
        query = f"""
        SELECT SUM(fs.saldo_contabil) as total_receitas_realizado
        FROM fato_saldos fs
        WHERE {coexercicio_column} = {placeholder}
          AND {inmes_column} IN ({placeholders_ate_bimestre or 'NULL'})
          AND fs.cocontacontabil BETWEEN '621200000' AND '621399999'
        """
        
        params = [self.ano] + self.meses_ate_bimestre
        df = self._executar_query_receitas(query, params)
        
        resultado = df['total_receitas_realizado'].iloc[0] if not df.empty else 0
        return resultado or 0

    def _get_despesas_liquidadas(self) -> float:
        """Busca total de despesas liquidadas até o bimestre."""
        env = get_db_environment()
        placeholder = '%s' if env == 'postgres' else '?'
        inmes_column = "CAST(fs.inmes AS INTEGER)" if env == 'postgres' else "fs.inmes"
        coexercicio_column = "CAST(fs.coexercicio AS INTEGER)" if env == 'postgres' else "fs.coexercicio"
        
        placeholders_ate_bimestre = ', '.join([placeholder] * len(self.meses_ate_bimestre))
        
        query = f"""
        SELECT SUM(fs.saldo_contabil_despesa) as total_despesas_liquidado
        FROM fato_saldo_despesa fs
        WHERE {coexercicio_column} = {placeholder}
          AND {inmes_column} IN ({placeholders_ate_bimestre or 'NULL'})
          AND fs.cocontacontabil IN ('622130300', '622130400', '622130700')
        """
        
        params = [self.ano] + self.meses_ate_bimestre
        df = self._executar_query_despesas(query, params)
        
        resultado = df['total_despesas_liquidado'].iloc[0] if not df.empty else 0
        return resultado or 0

    def calcular(self) -> dict:
        """
        Calcula superávit/déficit e retorna dados formatados para o template.
        
        Retorna:
            dict: {
                'receitas_realizadas': float,
                'despesas_liquidadas': float, 
                'diferenca': float,
                'tipo': 'superavit' | 'deficit',
                'valor_absoluto': float,
                'deficit_valor': float (0 se superávit),
                'superavit_valor': float (0 se déficit)
            }
        """
        receitas_realizadas = self._get_receitas_realizadas()
        despesas_liquidadas = self._get_despesas_liquidadas()
        
        diferenca = receitas_realizadas - despesas_liquidadas
        valor_absoluto = abs(diferenca)
        
        # Determinar tipo e valores para template
        if diferenca >= 0:
            # SUPERÁVIT
            tipo = 'superavit'
            deficit_valor = 0
            superavit_valor = valor_absoluto
        else:
            # DÉFICIT  
            tipo = 'deficit'
            deficit_valor = valor_absoluto
            superavit_valor = 0
        
        return {
            'receitas_realizadas': receitas_realizadas,
            'despesas_liquidadas': despesas_liquidadas,
            'diferenca': diferenca,
            'tipo': tipo,
            'valor_absoluto': valor_absoluto,
            'deficit_valor': deficit_valor,
            'superavit_valor': superavit_valor
        }