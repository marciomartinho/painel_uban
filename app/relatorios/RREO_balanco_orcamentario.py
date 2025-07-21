# app/relatorios/RREO_balanco_orcamentario.py
"""
Módulo para gerar o Demonstrativo da Execução Orçamentária da Receita.
Versão com a regra de cálculo de bimestres ACUMULADA para TODAS as colunas.
"""
import pandas as pd
# Certifique-se de que esta importação está correta no seu projeto
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

        # ---- INÍCIO DA CORREÇÃO PRINCIPAL ----
        # Adicionado o filtro de meses (até o bimestre) para as colunas de PREVISÃO
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
        # ---- FIM DA CORREÇÃO PRINCIPAL ----
        
        return self._executar_query(query, params)

    def _get_saldos_exercicios_anteriores(self) -> dict:
        """Busca os dados para as linhas de Saldos de Exercícios Anteriores."""
        env = get_db_environment()
        placeholder = '%s' if env == 'postgres' else '?'
        inmes_column = "CAST(fs.inmes AS INTEGER)" if env == 'postgres' else "fs.inmes"
        coexercicio_column = "CAST(fs.coexercicio AS INTEGER)" if env == 'postgres' else "fs.coexercicio"
        cocontacorrente_like = "fs.cocontacorrente LIKE %s" if env == 'postgres' else "fs.cocontacorrente LIKE ?"

        placeholders_no_bimestre = ', '.join([placeholder] * len(self.meses_apenas_no_bimestre))
        placeholders_ate_bimestre = ', '.join([placeholder] * len(self.meses_ate_bimestre))

        # Ajuste também na query de RPPS para garantir consistência
        query_rpps = f"""
        SELECT
            SUM(CASE WHEN {inmes_column} IN ({placeholders_ate_bimestre or 'NULL'}) AND fs.cocontacontabil BETWEEN '521100000' AND '521199999' THEN fs.saldo_contabil ELSE 0 END) as previsao_inicial,
            SUM(CASE WHEN {inmes_column} IN ({placeholders_ate_bimestre or 'NULL'}) AND fs.cocontacontabil BETWEEN '521100000' AND '521299999' THEN fs.saldo_contabil ELSE 0 END) as previsao_atualizada,
            SUM(CASE WHEN {inmes_column} IN ({placeholders_no_bimestre or 'NULL'}) AND fs.cocontacontabil BETWEEN '621200000' AND '621399999' THEN fs.saldo_contabil ELSE 0 END) as realizado_bimestre,
            SUM(CASE WHEN {inmes_column} IN ({placeholders_ate_bimestre or 'NULL'}) AND fs.cocontacontabil BETWEEN '621200000' AND '621399999' THEN fs.saldo_contabil ELSE 0 END) as realizado_ate_bimestre
        FROM fato_saldos fs
        WHERE {coexercicio_column} = {placeholder} AND {cocontacorrente_like}
        """
        params_rpps = self.meses_ate_bimestre + self.meses_ate_bimestre + self.meses_apenas_no_bimestre + self.meses_ate_bimestre + [self.ano, '99%']
        df_rpps = self._executar_query(query_rpps, params_rpps)

        query_superavit = f"""
        SELECT
            SUM(fs.saldo_contabil) as previsao_atualizada
        FROM fato_saldos fs
        WHERE {coexercicio_column} = {placeholder}
          AND {inmes_column} IN ({placeholders_ate_bimestre or 'NULL'})
          AND fs.cocontacontabil BETWEEN '522130100' AND '522130199'
        """
        params_superavit = [self.ano] + self.meses_ate_bimestre
        df_superavit = self._executar_query(query_superavit, params_superavit)

        return {
            'rpps': df_rpps.iloc[0] if not df_rpps.empty else pd.Series(dtype='float64'),
            'superavit': df_superavit.iloc[0] if not df_superavit.empty else pd.Series(dtype='float64')
        }

    def _processar_hierarquia(self, df: pd.DataFrame, tipo_receita_principal: str) -> list:
        linhas_relatorio = []
        if df.empty: return []
        total_principal = {k: 0 for k in ['previsao_inicial', 'previsao_atualizada', 'realizado_bimestre', 'realizado_ate_bimestre']}
        for cofonte, grupo_fonte in df.groupby('cofontereceita'):
            total_fonte = grupo_fonte.sum(numeric_only=True)
            for key in total_principal: total_principal[key] += total_fonte.get(key, 0)
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
        df_correntes = self._get_dados_base("fs.cofontereceita BETWEEN '11' AND '19'")
        linhas_correntes = self._processar_hierarquia(df_correntes, "RECEITAS CORRENTES")
        
        df_capital = self._get_dados_base("fs.cofontereceita BETWEEN '21' AND '29'")
        linhas_capital = self._processar_hierarquia(df_capital, "RECEITAS DE CAPITAL")
        
        total_correntes = linhas_correntes[0] if linhas_correntes else {}
        total_capital = linhas_capital[0] if linhas_capital else {}
        
        total_exceto_intra = {k: total_correntes.get(k, 0) + total_capital.get(k, 0) for k in total_correntes if isinstance(total_correntes.get(k), (int, float))}
        linha_total_exceto_intra = self._criar_linha(pd.Series(total_exceto_intra), "RECEITAS (EXCETO INTRA-ORÇAMENTÁRIAS) (I)", 'total_grupo', 0)
        
        df_intra = self._get_dados_base("fs.cofontereceita BETWEEN '71' AND '79'")
        linhas_intra = self._processar_hierarquia(df_intra, "RECEITAS (INTRA-ORÇAMENTÁRIAS) (II)")
        total_intra = linhas_intra[0] if linhas_intra else self._criar_linha(pd.Series(), "RECEITAS (INTRA-ORÇAMENTÁRIAS) (II)", 'principal', 0)

        total_receitas_iii = {k: total_exceto_intra.get(k, 0) + total_intra.get(k, 0) for k in total_exceto_intra}
        linha_total_receitas_iii = self._criar_linha(pd.Series(total_receitas_iii), "TOTAL DAS RECEITAS (III) = (I + II)", 'total_geral', 0)

        linha_deficit = self._criar_linha(pd.Series(dtype='float64'), "DÉFICIT (IV)", "white", 0)
        total_v = {k: total_receitas_iii.get(k, 0) + linha_deficit.get(k, 0) for k in total_receitas_iii}
        linha_total_v = self._criar_linha(pd.Series(total_v), "TOTAL (V) = (III + IV)", "total_geral", 0)

        saldos_anteriores_data = self._get_saldos_exercicios_anteriores()

        dados_rpps = saldos_anteriores_data.get('rpps', pd.Series(dtype='float64'))
        linha_rpps = self._criar_linha(dados_rpps, "Recursos Arrecadados em Exercícios Anteriores - RPPS", "white-child", 1, "saldos_parent")
        
        dados_superavit_query = saldos_anteriores_data.get('superavit', pd.Series(dtype='float64')).fillna(0)
        valor_superavit = dados_superavit_query.get('previsao_atualizada', 0)
        dados_superavit_completos = pd.Series({
            'previsao_inicial': 0.00, 'previsao_atualizada': valor_superavit,
            'realizado_bimestre': 0.00, 'realizado_ate_bimestre': valor_superavit
        })
        linha_superavit = self._criar_linha(dados_superavit_completos, "Superávit Financeiro Utilizado para Créditos Adicionais", "white-child", 1, "saldos_parent")
        
        soma_saldos = pd.Series({
            'previsao_inicial': linha_rpps['previsao_inicial'] + linha_superavit['previsao_inicial'],
            'previsao_atualizada': linha_rpps['previsao_atualizada'] + linha_superavit['previsao_atualizada'],
            'realizado_bimestre': linha_rpps['realizado_bimestre'] + linha_superavit['realizado_bimestre'],
            'realizado_ate_bimestre': linha_rpps['realizado_ate_bimestre'] + linha_superavit['realizado_ate_bimestre'],
        })
        linha_saldos_exercicios_anteriores = self._criar_linha(soma_saldos, "SALDOS DE EXERCÍCIOS ANTERIORES", "white-parent", 0, "saldos_parent")

        return {
            'linhas_correntes': linhas_correntes, 'linhas_capital': linhas_capital,
            'linhas_intra': linhas_intra, 'total_intra': total_intra,
            'total_exceto_intra': linha_total_exceto_intra, 
            'total_receitas_iii': linha_total_receitas_iii,
            'linha_deficit': linha_deficit, 'total_v': linha_total_v,
            'saldos_exercicios_anteriores': linha_saldos_exercicios_anteriores,
            'linha_rpps': linha_rpps, 'linha_superavit': linha_superavit,
            'ano': self.ano, 'bimestre': self.bimestre
        }