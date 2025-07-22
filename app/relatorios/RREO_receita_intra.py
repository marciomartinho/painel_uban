# app/relatorios/RREO_receita_intra.py
"""
Módulo para gerar o Demonstrativo da Execução Orçamentária da Receita Intra-Orçamentária.
Focado apenas nas receitas intra (fontes 71-79).
"""
import pandas as pd
from ..modulos.conexao_hibrida import ConexaoBanco, adaptar_query, get_db_environment

class BalancoOrcamentarioReceitaIntraAnexo2:
    """
    Gera os dados para o Balanço Orçamentário da Receita Intra-Orçamentária, com lógica de
    cálculo bimestral específica para valores "no bimestre" e "até o bimestre".
    """

    def __init__(self, ano: int, bimestre: int):
        self.ano = ano
        self.bimestre = bimestre
        self.bimestre_map = {
            1: [1, 2], 2: [3, 4], 3: [5, 6],
            4: [7, 8], 5: [9, 10], 6: [11, 12]
        }
        
        self.meses_apenas_no_bimestre = self.bimestre_map.get(self.bimestre, [])
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
        placeholder = '%s' if env == 'postgres' else '?'
        inmes_column = "CAST(fs.inmes AS INTEGER)" if env == 'postgres' else "fs.inmes"
        coexercicio_column = "CAST(fs.coexercicio AS INTEGER)" if env == 'postgres' else "fs.coexercicio"
        
        placeholders_no_bimestre = ', '.join([placeholder] * len(self.meses_apenas_no_bimestre))
        placeholders_ate_bimestre = ', '.join([placeholder] * len(self.meses_ate_bimestre))
        
        # Os parâmetros agora são usados para todas as colunas
        params = self.meses_ate_bimestre + self.meses_ate_bimestre + self.meses_apenas_no_bimestre + self.meses_ate_bimestre + [self.ano]
        
        type_cast = "::text" if env == 'postgres' else ""

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
            WHERE {coexercicio_column} = {placeholder} AND ({tipo_receita_sql})
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
        """Processa a hierarquia de receitas intra-orçamentárias"""
        linhas_relatorio = []
        if df.empty: 
            return []
            
        total_principal = {k: 0 for k in ['previsao_inicial', 'previsao_atualizada', 'realizado_bimestre', 'realizado_ate_bimestre']}
        
        for cofonte, grupo_fonte in df.groupby('cofontereceita'):
            total_fonte = grupo_fonte.sum(numeric_only=True)
            for key in total_principal: 
                total_principal[key] += total_fonte.get(key, 0)
                
            nome_fonte = grupo_fonte['nofontereceita'].iloc[0] or f"Fonte {cofonte}"
            
            if len(grupo_fonte) == 1 and grupo_fonte['cosubfontereceita'].iloc[0] is not None:
                linhas_relatorio.append(self._criar_linha(total_fonte, nome_fonte.upper(), 'fonte_sozinha', 1))
            else:
                linhas_relatorio.append(self._criar_linha(total_fonte, nome_fonte.upper(), 'fonte', 1, cofonte))
                for _, linha_subfonte in grupo_fonte.iterrows():
                    nome_subfonte = linha_subfonte['nosubfontereceita'] or f"Subfonte {linha_subfonte['cosubfontereceita']}"
                    linhas_relatorio.append(self._criar_linha(linha_subfonte, nome_subfonte, 'subfonte', 2, cofonte))
        
        linhas_relatorio.insert(0, self._criar_linha(pd.Series(total_principal), tipo_receita_principal, 'principal', 0))
        return linhas_relatorio

    def _criar_linha(self, dados_serie: pd.Series, descricao: str, tipo: str, nivel: int, pai_id: str = None) -> dict:
        """Cria uma linha formatada para o relatório"""
        dados_serie = dados_serie.fillna(0)
        previsao_atualizada = dados_serie.get('previsao_atualizada', 0)
        realizado_bimestre = dados_serie.get('realizado_bimestre', 0)
        realizado_ate_bimestre = dados_serie.get('realizado_ate_bimestre', 0)
        
        return {
            'descricao': descricao, 'tipo': tipo, 'nivel': nivel, 'pai_id': pai_id,
            'previsao_inicial': dados_serie.get('previsao_inicial', 0),
            'previsao_atualizada': previsao_atualizada, 
            'realizado_bimestre': realizado_bimestre,
            'pct_bimestre': (realizado_bimestre / previsao_atualizada * 100) if previsao_atualizada else 0,
            'realizado_ate_bimestre': realizado_ate_bimestre,
            'pct_ate_bimestre': (realizado_ate_bimestre / previsao_atualizada * 100) if previsao_atualizada else 0,
            'saldo': previsao_atualizada - realizado_ate_bimestre
        }

    def gerar_relatorio(self) -> dict:
        """Gera o relatório completo de receitas intra-orçamentárias"""
        # Apenas receitas correntes intra (fontes 71-79)
        df_correntes_intra = self._get_dados_base("fs.cofontereceita BETWEEN '71' AND '79'")
        linhas_correntes_intra = self._processar_hierarquia(df_correntes_intra, "RECEITAS CORRENTES INTRA-ORÇAMENTÁRIAS")
        
        # Total das receitas intra
        total_intra = linhas_correntes_intra[0] if linhas_correntes_intra else self._criar_linha(pd.Series(), "RECEITAS (INTRA-ORÇAMENTÁRIAS) (II)", 'total_geral', 0)
        
        return {
            'linhas_correntes_intra': linhas_correntes_intra,
            'total_intra': total_intra,
            'ano': self.ano,
            'bimestre': self.bimestre
        }