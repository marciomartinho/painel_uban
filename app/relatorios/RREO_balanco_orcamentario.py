# app/relatorios/RREO_balanco_orcamentario.py
"""
Módulo para gerar o Demonstrativo da Execução Orçamentária da Receita.
Versão com a regra de cálculo de bimestres ACUMULADA para TODAS as colunas.
"""
import pandas as pd
from ..modulos.conexao_hibrida import ConexaoBanco, adaptar_query, get_db_environment

class BalancoOrcamentarioAnexo2:
    """
    Gera os dados para o Balanço Orçamentário da Receita, com lógica de
    cálculo bimestral específica para valores "no bimestre" e "até o bimestre".
    """

    def __init__(self, ano: int, bimestre: int):
        self.ano = ano
        self.bimestre = bimestre
        self.bimestre_map = {
            1: [1, 2], 2: [3, 4], 3: [5, 6],
            4: [7, 8], 5: [9, 10], 6: [11, 12]
        }
        
        # Meses APENAS para o bimestre selecionado (para a coluna "NO BIMESTRE")
        self.meses_apenas_no_bimestre = self.bimestre_map.get(self.bimestre, [])
        
        # Meses ACUMULADOS até o bimestre selecionado (para as outras colunas)
        self.meses_ate_bimestre = []
        for i in range(1, self.bimestre + 1):
            self.meses_ate_bimestre.extend(self.bimestre_map.get(i, []))

    def _executar_query(self, query: str, params=None) -> pd.DataFrame:
        """Executa uma query e retorna um DataFrame do Pandas."""
        with ConexaoBanco() as conn:
            query_adaptada = adaptar_query(query)
            df = pd.read_sql_query(query_adaptada, conn, params=params)
            df.columns = [col.lower() for col in df.columns]
            return df

    def _get_dados_base(self, tipo_receita_sql: str) -> pd.DataFrame:
        """Busca e calcula os valores base do banco de dados com a nova lógica de bimestres."""
        env = get_db_environment()
        placeholder = '%' if env == 'postgres' else '?'
        inmes_column = "CAST(fs.inmes AS INTEGER)" if env == 'postgres' else "fs.inmes"
        coexercicio_column = "CAST(fs.coexercicio AS INTEGER)" if env == 'postgres' else "fs.coexercicio"
        
        placeholders_no_bimestre = ', '.join([placeholder] * len(self.meses_apenas_no_bimestre))
        placeholders_ate_bimestre = ', '.join([placeholder] * len(self.meses_ate_bimestre))
        
        params = self.meses_ate_bimestre + self.meses_ate_bimestre + self.meses_apenas_no_bimestre + self.meses_ate_bimestre + [self.ano]
        
        type_cast = "::text" if env == 'postgres' else ""

        # --- QUERY CORRIGIDA ---
        # Adicionados parênteses em volta de {tipo_receita_sql} na cláusula WHERE
        query = f"""
        WITH saldos_agregados AS (
            SELECT
                fs.cofontereceita,
                fs.cosubfontereceita,
                SUM(CASE WHEN {inmes_column} IN ({placeholders_ate_bimestre or 'NULL'}) AND fs.cocontacontabil BETWEEN '521100000' AND '521199999' THEN fs.saldo_contabil ELSE 0 END) as previsao_inicial,
                SUM(CASE WHEN {inmes_column} IN ({placeholders_ate_bimestre or 'NULL'}) AND fs.cocontacontabil BETWEEN '521100000' AND '521299999' THEN fs.saldo_contabil ELSE 0 END) as previsao_atualizada,
                SUM(CASE WHEN {inmes_column} IN ({placeholders_no_bimestre or 'NULL'}) AND fs.cocontacontabil BETWEEN '621200000' AND '621399999' THEN fs.saldo_contabil ELSE 0 END) as realizado_bimestre,
                SUM(CASE WHEN {inmes_column} IN ({placeholders_ate_bimestre or 'NULL'}) AND fs.cocontacontabil BETWEEN '621200000' AND '621399999' THEN fs.saldo_contabil ELSE 0 END) as realizado_ate_bimestre
            FROM fato_saldos fs
            WHERE {coexercicio_column} = {placeholder} AND ({tipo_receita_sql}) -- <<< PARÊNTESES ADICIONADOS AQUI
            GROUP BY fs.cofontereceita, fs.cosubfontereceita
        )
        SELECT
            sa.*,
            ori.nofontereceita,
            esp.nosubfontereceita
        FROM saldos_agregados sa
        LEFT JOIN dimensoes.origens ori ON sa.cofontereceita{type_cast} = ori.cofontereceita
        LEFT JOIN dimensoes.especies esp ON sa.cosubfontereceita{type_cast} = esp.cosubfontereceita
        WHERE sa.previsao_atualizada != 0 OR sa.realizado_ate_bimestre != 0
        ORDER BY sa.cofontereceita, sa.cosubfontereceita;
        """
        
        return self._executar_query(query, params)

    def _processar_hierarquia(self, df: pd.DataFrame, tipo_receita_principal: str) -> list:
        linhas_relatorio = []
        if df.empty: return []
        total_principal = {k: 0 for k in ['previsao_inicial', 'previsao_atualizada', 'realizado_bimestre', 'realizado_ate_bimestre']}
        for cofonte, grupo_fonte in df.groupby('cofontereceita'):
            total_fonte = grupo_fonte.sum(numeric_only=True)
            for key in total_principal: total_principal[key] += total_fonte.get(key, 0)
            nome_fonte = grupo_fonte['nofontereceita'].iloc[0] or f"Fonte {cofonte}"
            if len(grupo_fonte) == 1:
                linhas_relatorio.append(self._criar_linha(total_fonte, nome_fonte.upper(), 'fonte_sozinha', 1))
            else:
                linhas_relatorio.append(self._criar_linha(total_fonte, nome_fonte.upper(), 'fonte', 1, cofonte))
                for _, linha_subfonte in grupo_fonte.iterrows():
                    nome_subfonte = linha_subfonte['nosubfontereceita'] or f"Subfonte {linha_subfonte['cosubfontereceita']}"
                    linhas_relatorio.append(self._criar_linha(linha_subfonte, nome_subfonte, 'subfonte', 2, cofonte))
        linhas_relatorio.insert(0, self._criar_linha(pd.Series(total_principal), tipo_receita_principal, 'principal', 0))
        return linhas_relatorio

    def _criar_linha(self, dados_serie: pd.Series, descricao: str, tipo: str, nivel: int, pai_id: str = None) -> dict:
        previsao_atualizada = dados_serie.get('previsao_atualizada', 0)
        realizado_bimestre = dados_serie.get('realizado_bimestre', 0)
        realizado_ate_bimestre = dados_serie.get('realizado_ate_bimestre', 0)
        return {
            'descricao': descricao, 'tipo': tipo, 'nivel': nivel, 'pai_id': pai_id,
            'previsao_inicial': dados_serie.get('previsao_inicial', 0),
            'previsao_atualizada': previsao_atualizada, 'realizado_bimestre': realizado_bimestre,
            'pct_bimestre': (realizado_bimestre / previsao_atualizada * 100) if previsao_atualizada else 0,
            'realizado_ate_bimestre': realizado_ate_bimestre,
            'pct_ate_bimestre': (realizado_ate_bimestre / previsao_atualizada * 100) if previsao_atualizada else 0,
            'saldo': previsao_atualizada - realizado_ate_bimestre
        }

    def gerar_relatorio(self) -> dict:
        df_correntes = self._get_dados_base("fs.cofontereceita BETWEEN '11' AND '19'")
        linhas_correntes = self._processar_hierarquia(df_correntes, "RECEITAS CORRENTES")
        df_capital = self._get_dados_base("fs.cofontereceita BETWEEN '21' AND '29'")
        linhas_capital = self._processar_hierarquia(df_capital, "RECEITAS DE CAPITAL")
        total_correntes = linhas_correntes[0] if linhas_correntes else {}
        total_capital = linhas_capital[0] if linhas_capital else {}
        total_exceto_intra = {k: total_correntes.get(k, 0) + total_capital.get(k, 0) for k in total_correntes if isinstance(total_correntes.get(k), (int, float))}
        linha_total_exceto_intra = self._criar_linha(pd.Series(total_exceto_intra), "RECEITAS (EXCETO INTRA-ORÇAMENTÁRIAS) (I)", 'total_grupo', 0)
        df_intra = self._get_dados_base("fs.cofontereceita LIKE '7%' OR fs.cofontereceita LIKE '8%'")
        total_intra = self._criar_linha(df_intra.sum(numeric_only=True), "RECEITAS (INTRA-ORÇAMENTÁRIAS) (II)", 'principal', 0)
        total_geral_valores = {k: total_exceto_intra.get(k, 0) + total_intra.get(k, 0) for k in total_exceto_intra}
        total_geral = self._criar_linha(pd.Series(total_geral_valores), "TOTAL DAS RECEITAS (I + II)", 'total_geral', 0)
        
        return {
            'linhas_correntes': linhas_correntes, 'linhas_capital': linhas_capital,
            'total_exceto_intra': linha_total_exceto_intra, 'total_intra': total_intra,
            'total_geral': total_geral, 'ano': self.ano, 'bimestre': self.bimestre
        }