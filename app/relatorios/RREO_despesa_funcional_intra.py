# app/relatorios/RREO_despesa_funcional_intra.py
"""
Módulo para gerar o Demonstrativo da Execução Orçamentária da Despesa Intra-Orçamentária por Função.
Focado apenas nas despesas intra (modalidade 91) organizadas por função/subfunção.
"""
import pandas as pd
from ..modulos.conexao_hibrida import ConexaoBanco, adaptar_query, get_db_environment

class BalancoOrcamentarioDespesaFuncionalIntraAnexo2:
    """
    Gera os dados para o Balanço Orçamentário da Despesa Intra-Orçamentária por Função, com lógica de
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
        with ConexaoBanco(db_name='saldos_despesa') as conn:
            query_adaptada = adaptar_query(query)
            df = pd.read_sql_query(query_adaptada, conn, params=params)
            df.columns = [col.lower() for col in df.columns]
            return df

    def _get_dados_base_funcional(self, filtro_modalidade: str, filtro_funcao: str = None, filtro_subfuncao: str = None) -> pd.DataFrame:
        """Busca e calcula os valores base do banco de dados por função/subfunção."""
        env = get_db_environment()
        placeholder = '%s' if env == 'postgres' else '?'
        inmes_column = "CAST(fs.inmes AS INTEGER)" if env == 'postgres' else "fs.inmes"
        coexercicio_column = "CAST(fs.coexercicio AS INTEGER)" if env == 'postgres' else "fs.coexercicio"
        
        # Criar placeholders para cada uso
        placeholders_no_bimestre = ', '.join([placeholder] * len(self.meses_apenas_no_bimestre))
        placeholders_ate_bimestre = ', '.join([placeholder] * len(self.meses_ate_bimestre))
        
        # Se não houver meses, usar NULL para evitar erro SQL
        if not self.meses_apenas_no_bimestre:
            placeholders_no_bimestre = 'NULL'
        if not self.meses_ate_bimestre:
            placeholders_ate_bimestre = 'NULL'
        
        # Montar parâmetros na ordem exata da query
        params = []
        
        # dotacao_inicial: ate_bimestre
        if self.meses_ate_bimestre:
            params.extend(self.meses_ate_bimestre)
        
        # dotacao_autorizada: ate_bimestre
        if self.meses_ate_bimestre:
            params.extend(self.meses_ate_bimestre)
        
        # empenhado_bimestre: apenas_no_bimestre
        if self.meses_apenas_no_bimestre:
            params.extend(self.meses_apenas_no_bimestre)
        
        # empenhado_ate_bimestre: ate_bimestre
        if self.meses_ate_bimestre:
            params.extend(self.meses_ate_bimestre)
        
        # liquidado_bimestre: apenas_no_bimestre
        if self.meses_apenas_no_bimestre:
            params.extend(self.meses_apenas_no_bimestre)
        
        # liquidado_ate_bimestre: ate_bimestre
        if self.meses_ate_bimestre:
            params.extend(self.meses_ate_bimestre)
        
        # pago_ate_bimestre: ate_bimestre
        if self.meses_ate_bimestre:
            params.extend(self.meses_ate_bimestre)
        
        # WHERE: ano
        params.append(self.ano)
        
        # Filtros de função e subfunção se especificados
        filtro_funcao_sql = ""
        if filtro_funcao:
            filtro_funcao_sql = f" AND fs.cofuncao = {placeholder}"
            params.append(filtro_funcao)
            
        filtro_subfuncao_sql = ""
        if filtro_subfuncao:
            filtro_subfuncao_sql = f" AND fs.cosubfuncao = {placeholder}"
            params.append(filtro_subfuncao)

        query = f"""
        WITH saldos_agregados AS (
            SELECT
                fs.cofuncao,
                fs.cosubfuncao,
                SUM(CASE 
                    WHEN {inmes_column} IN ({placeholders_ate_bimestre or 'NULL'}) 
                    AND fs.cocontacontabil BETWEEN '522110000' AND '522119999' 
                    THEN fs.saldo_contabil_despesa
                    ELSE 0 
                END) as dotacao_inicial,
                
                SUM(CASE 
                    WHEN {inmes_column} IN ({placeholders_ate_bimestre or 'NULL'}) 
                    AND (
                        (fs.cocontacontabil BETWEEN '522110000' AND '522129999') OR
                        (fs.cocontacontabil BETWEEN '522150000' AND '522159999') OR
                        (fs.cocontacontabil BETWEEN '522190000' AND '522199999')
                    )
                    THEN fs.saldo_contabil_despesa
                    ELSE 0 
                END) as dotacao_autorizada,
                
                SUM(CASE 
                    WHEN {inmes_column} IN ({placeholders_no_bimestre or 'NULL'}) 
                    AND fs.cocontacontabil BETWEEN '622130000' AND '622139999'
                    THEN fs.saldo_contabil_despesa
                    ELSE 0 
                END) as empenhado_bimestre,
                
                SUM(CASE 
                    WHEN {inmes_column} IN ({placeholders_ate_bimestre or 'NULL'}) 
                    AND fs.cocontacontabil BETWEEN '622130000' AND '622139999'
                    THEN fs.saldo_contabil_despesa
                    ELSE 0 
                END) as empenhado_ate_bimestre,
                
                SUM(CASE 
                    WHEN {inmes_column} IN ({placeholders_no_bimestre or 'NULL'}) 
                    AND fs.cocontacontabil IN ('622130300', '622130400', '622130700')
                    THEN fs.saldo_contabil_despesa
                    ELSE 0 
                END) as liquidado_bimestre,
                
                SUM(CASE 
                    WHEN {inmes_column} IN ({placeholders_ate_bimestre or 'NULL'}) 
                    AND fs.cocontacontabil IN ('622130300', '622130400', '622130700')
                    THEN fs.saldo_contabil_despesa
                    ELSE 0 
                END) as liquidado_ate_bimestre,
                
                SUM(CASE 
                    WHEN {inmes_column} IN ({placeholders_ate_bimestre or 'NULL'}) 
                    AND fs.cocontacontabil = '622920104'
                    THEN fs.saldo_contabil_despesa
                    ELSE 0 
                END) as pago_ate_bimestre
                
            FROM fato_saldo_despesa fs
            WHERE {coexercicio_column} = {placeholder} 
            AND {filtro_modalidade}
            {filtro_funcao_sql}
            {filtro_subfuncao_sql}
            GROUP BY fs.cofuncao, fs.cosubfuncao
        )
        SELECT * FROM saldos_agregados
        WHERE (
            ABS(dotacao_inicial) + ABS(dotacao_autorizada) + 
            ABS(empenhado_ate_bimestre) + ABS(liquidado_ate_bimestre) + 
            ABS(pago_ate_bimestre)
        ) > 0.01
        ORDER BY cofuncao, cosubfuncao;
        """
        
        return self._executar_query(query, params)

    def _criar_linha(self, dados_serie: pd.Series, descricao: str, tipo: str, nivel: int, pai_id: str = None) -> dict:
        """Cria uma linha formatada para o relatório"""
        dados_serie = dados_serie.fillna(0)
        
        dotacao_inicial = dados_serie.get('dotacao_inicial', 0)
        dotacao_autorizada = dados_serie.get('dotacao_autorizada', 0)
        empenhado_bimestre = dados_serie.get('empenhado_bimestre', 0)
        empenhado_ate_bimestre = dados_serie.get('empenhado_ate_bimestre', 0)
        liquidado_bimestre = dados_serie.get('liquidado_bimestre', 0)
        liquidado_ate_bimestre = dados_serie.get('liquidado_ate_bimestre', 0)
        pago_ate_bimestre = dados_serie.get('pago_ate_bimestre', 0)
        
        # Cálculo dos saldos conforme as novas regras
        saldo_empenhado = dotacao_autorizada - empenhado_ate_bimestre
        saldo_liquidado = dotacao_autorizada - liquidado_ate_bimestre
        
        return {
            'descricao': descricao,
            'tipo': tipo,
            'nivel': nivel,
            'pai_id': pai_id,
            'dotacao_inicial': dotacao_inicial,
            'dotacao_autorizada': dotacao_autorizada,
            'empenhado_bimestre': empenhado_bimestre,
            'empenhado_ate_bimestre': empenhado_ate_bimestre,
            'saldo_empenhado': saldo_empenhado,
            'liquidado_bimestre': liquidado_bimestre,
            'liquidado_ate_bimestre': liquidado_ate_bimestre,
            'saldo_liquidado': saldo_liquidado,
            'pago_ate_bimestre': pago_ate_bimestre
        }

    def _processar_despesas_intra_por_funcao(self, filtro_modalidade: str) -> tuple:
        """Processa despesas intra-orçamentárias agrupadas por função e subfunção"""
        linhas = []
        total_grupo = pd.Series({
            'dotacao_inicial': 0,
            'dotacao_autorizada': 0,
            'empenhado_bimestre': 0,
            'empenhado_ate_bimestre': 0,
            'liquidado_bimestre': 0,
            'liquidado_ate_bimestre': 0,
            'pago_ate_bimestre': 0
        })
        
        # Busca todas as funções com movimento intra-orçamentário
        df_funcoes = self._get_dados_base_funcional(filtro_modalidade)
        
        if not df_funcoes.empty:
            # Obtém nomes das funções e subfunções
            df_funcoes = self._enriquecer_com_nomes_funcoes(df_funcoes)
            
            # Agrupa por função
            funcoes_agrupadas = df_funcoes.groupby(['cofuncao', 'nofuncao']).agg({
                'dotacao_inicial': 'sum',
                'dotacao_autorizada': 'sum',
                'empenhado_bimestre': 'sum',
                'empenhado_ate_bimestre': 'sum',
                'liquidado_bimestre': 'sum',
                'liquidado_ate_bimestre': 'sum',
                'pago_ate_bimestre': 'sum'
            }).reset_index()
            
            for _, funcao_row in funcoes_agrupadas.iterrows():
                # Adiciona linha da função
                linha_funcao = self._criar_linha(
                    funcao_row,
                    f"{funcao_row['cofuncao']} - {funcao_row['nofuncao']}",
                    'fonte',
                    0,
                    None
                )
                linhas.append(linha_funcao)
                
                # Adiciona subfunções desta função
                subfuncoes_da_funcao = df_funcoes[df_funcoes['cofuncao'] == funcao_row['cofuncao']]
                for _, subfuncao_row in subfuncoes_da_funcao.iterrows():
                    linha_subfuncao = self._criar_linha(
                        subfuncao_row,
                        f"{subfuncao_row['cosubfuncao']} - {subfuncao_row['nosubfuncao']}",
                        'subfonte',
                        1,
                        str(funcao_row['cofuncao'])
                    )
                    linhas.append(linha_subfuncao)
                
                # Soma ao total do grupo
                for key in total_grupo.index:
                    total_grupo[key] += funcao_row.get(key, 0)
        
        return total_grupo, linhas

    def _enriquecer_com_nomes_funcoes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Busca os nomes das funções e subfunções"""
        try:
            # Busca nomes das funções
            with ConexaoBanco(db_name='dimensoes') as conn:
                env = get_db_environment()
                placeholder = '%s' if env == 'postgres' else '?'
                
                # Lista de códigos únicos de funções
                codigos_funcao = [str(f) for f in df['cofuncao'].unique()]
                placeholders_funcao = ', '.join([placeholder] * len(codigos_funcao))
                
                query_funcoes = f"""
                SELECT cofuncao, nofuncao 
                FROM funcoes 
                WHERE cofuncao IN ({placeholders_funcao})
                """
                df_func_nomes = pd.read_sql_query(adaptar_query(query_funcoes), conn, params=codigos_funcao)
                df_func_nomes.columns = [col.lower() for col in df_func_nomes.columns]
                df_func_nomes['cofuncao'] = df_func_nomes['cofuncao'].astype(str)
                
                # Lista de códigos únicos de subfunções
                codigos_subfuncao = [str(f) for f in df['cosubfuncao'].unique()]
                placeholders_subfuncao = ', '.join([placeholder] * len(codigos_subfuncao))
                
                query_subfuncoes = f"""
                SELECT cosubfuncao, nosubfuncao 
                FROM subfuncoes 
                WHERE cosubfuncao IN ({placeholders_subfuncao})
                """
                df_subfunc_nomes = pd.read_sql_query(adaptar_query(query_subfuncoes), conn, params=codigos_subfuncao)
                df_subfunc_nomes.columns = [col.lower() for col in df_subfunc_nomes.columns]
                df_subfunc_nomes['cosubfuncao'] = df_subfunc_nomes['cosubfuncao'].astype(str)
                
            # Converte colunas para string para merge
            df['cofuncao'] = df['cofuncao'].astype(str)
            df['cosubfuncao'] = df['cosubfuncao'].astype(str)
            
            # Faz o merge
            df = df.merge(df_func_nomes, on='cofuncao', how='left')
            df = df.merge(df_subfunc_nomes, on='cosubfuncao', how='left')
            
            # Preenche nomes faltantes
            df['nofuncao'] = df['nofuncao'].fillna('Função ' + df['cofuncao'])
            df['nosubfuncao'] = df['nosubfuncao'].fillna('Subfunção ' + df['cosubfuncao'])
            
        except Exception as e:
            print(f"Erro ao buscar nomes de funções: {e}")
            df['nofuncao'] = 'Função ' + df['cofuncao'].astype(str)
            df['nosubfuncao'] = 'Subfunção ' + df['cosubfuncao'].astype(str)
            
        return df

    def gerar_relatorio(self) -> dict:
        """Gera o relatório completo de despesas intra-orçamentárias por função"""
        # Filtro para modalidade intra-orçamentária
        filtro_intra = "fs.comodalidade = '91'"
        
        # DESPESAS INTRA-ORÇAMENTÁRIAS POR FUNÇÃO
        total_intra, linhas_intra = self._processar_despesas_intra_por_funcao(filtro_intra)
        
        linha_total_intra = self._criar_linha(
            total_intra,
            'DESPESAS (INTRA-ORÇAMENTÁRIAS) (II)',
            'total_geral',
            0
        )
        
        return {
            'total_intra': linha_total_intra,
            'linhas_intra': linhas_intra,
            'ano': self.ano,
            'bimestre': self.bimestre
        }