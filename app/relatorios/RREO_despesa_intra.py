# app/relatorios/RREO_despesa_intra.py
"""
Módulo para gerar o Demonstrativo da Execução Orçamentária da Despesa Intra-Orçamentária.
Focado apenas nas despesas intra (modalidade 91).
"""
import pandas as pd
from ..modulos.conexao_hibrida import ConexaoBanco, adaptar_query, get_db_environment

class BalancoOrcamentarioDespesaIntraAnexo2:
    """
    Gera os dados para o Balanço Orçamentário da Despesa Intra-Orçamentária, com lógica de
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

    def _get_dados_base(self, filtro_modalidade: str, filtro_categoria: str = None) -> pd.DataFrame:
        """Busca e calcula os valores base do banco de dados com a nova lógica de bimestres."""
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
        
        # Filtro de categoria se especificado
        filtro_categoria_sql = ""
        if filtro_categoria:
            filtro_categoria_sql = f" AND fs.incategoria = {placeholder}"
            params.append(filtro_categoria)

        query = f"""
        WITH saldos_agregados AS (
            SELECT
                fs.incategoria,
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
            {filtro_categoria_sql}
            GROUP BY fs.incategoria
        )
        SELECT * FROM saldos_agregados
        WHERE (
            ABS(dotacao_inicial) + ABS(dotacao_autorizada) + 
            ABS(empenhado_ate_bimestre) + ABS(liquidado_ate_bimestre) + 
            ABS(pago_ate_bimestre)
        ) > 0.01
        ORDER BY incategoria;
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

    def _processar_grupo_despesas(self, filtro_modalidade: str, categorias: list, nome_grupo: str) -> tuple:
        """Processa um grupo de despesas (correntes ou capital) intra-orçamentárias"""
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
        
        # Busca dados para cada categoria
        for cat in categorias:
            df_cat = self._get_dados_base(filtro_modalidade, str(cat))
            if not df_cat.empty:
                dados_cat = df_cat.sum(numeric_only=True)
                
                # Mapeia categorias para descrições
                descricoes = {
                    '1': 'PESSOAL E ENCARGOS SOCIAIS',
                    '2': 'JUROS E ENCARGOS DA DÍVIDA',
                    '3': 'OUTRAS DESPESAS CORRENTES',
                    '4': 'INVESTIMENTOS',
                    '5': 'INVERSÕES FINANCEIRAS',
                    '6': 'AMORTIZAÇÃO DA DÍVIDA'
                }
                
                linha = self._criar_linha(
                    dados_cat,
                    descricoes.get(str(cat), f'Categoria {cat}'),
                    'subfonte',
                    1,
                    nome_grupo.lower().replace(' ', '_')
                )
                linhas.append(linha)
                
                # Soma ao total do grupo
                for key in total_grupo.index:
                    total_grupo[key] += dados_cat.get(key, 0)
        
        # Cria linha do total do grupo
        linha_total = self._criar_linha(total_grupo, nome_grupo, 'fonte', 0, None)
        
        return linha_total, linhas

    def gerar_relatorio(self) -> dict:
        """Gera o relatório completo de despesas intra-orçamentárias"""
        # Filtro para modalidade intra-orçamentária
        filtro_intra = "fs.comodalidade = '91'"
        
        # DESPESAS CORRENTES INTRA (categorias 1, 2, 3 com modalidade 91)
        total_correntes_intra, linhas_correntes_intra = self._processar_grupo_despesas(
            filtro_intra, 
            ['1', '2', '3'], 
            'DESPESAS CORRENTES INTRA-ORÇAMENTÁRIAS'
        )
        
        # DESPESAS DE CAPITAL INTRA (categorias 4, 5, 6 com modalidade 91)
        total_capital_intra, linhas_capital_intra = self._processar_grupo_despesas(
            filtro_intra, 
            ['4', '5', '6'], 
            'DESPESAS DE CAPITAL INTRA-ORÇAMENTÁRIAS'
        )
        
        # TOTAL DAS DESPESAS INTRA
        total_despesas_intra = pd.Series()
        for key in total_correntes_intra.keys():
            if key not in ['descricao', 'tipo', 'nivel', 'pai_id']:
                total_despesas_intra[key] = total_correntes_intra[key] + total_capital_intra[key]
        
        linha_total_despesas_intra = self._criar_linha(
            total_despesas_intra,
            'DESPESAS (INTRA-ORÇAMENTÁRIAS) (IX)',
            'total_geral',
            0
        )
        
        return {
            'total_correntes_intra': total_correntes_intra,
            'linhas_correntes_intra': linhas_correntes_intra,
            'total_capital_intra': total_capital_intra,
            'linhas_capital_intra': linhas_capital_intra,
            'total_despesas_intra': linha_total_despesas_intra,
            'ano': self.ano,
            'bimestre': self.bimestre
        }