# app/relatorios/RREO_balanco_intra.py
"""
Módulo para gerar o Demonstrativo Completo do Balanço Orçamentário Intra-Orçamentário.
Combina os dados de receitas e despesas intra-orçamentárias em um único relatório.
"""
import pandas as pd
from ..modulos.conexao_hibrida import ConexaoBanco, adaptar_query, get_db_environment
from .RREO_receita_intra import BalancoOrcamentarioReceitaIntraAnexo2
from .RREO_despesa_intra import BalancoOrcamentarioDespesaIntraAnexo2

class BalancoOrcamentarioIntraAnexo2:
    """
    Gera os dados para o Balanço Orçamentário Intra-Orçamentário (Receitas e Despesas),
    com lógica de cálculo bimestral específica para valores "no bimestre" e "até o bimestre".
    """

    def __init__(self, ano: int, bimestre: int):
        self.ano = ano
        self.bimestre = bimestre

    def gerar_relatorio(self) -> dict:
        """Gera o relatório completo de receitas e despesas intra-orçamentárias"""
        
        # --- PARTE 1: RECEITAS INTRA ---
        receita_builder = BalancoOrcamentarioReceitaIntraAnexo2(self.ano, self.bimestre)
        dados_receita = receita_builder.gerar_relatorio()

        # --- PARTE 2: DESPESAS INTRA ---
        despesa_builder = BalancoOrcamentarioDespesaIntraAnexo2(self.ano, self.bimestre)
        dados_despesa = despesa_builder.gerar_relatorio()

        return {
            # Dados de Receita Intra
            'receita_linhas_correntes_intra': dados_receita['linhas_correntes_intra'],
            'receita_total_intra': dados_receita['total_intra'],
            
            # Dados de Despesa Intra
            'despesa_total_correntes_intra': dados_despesa['total_correntes_intra'],
            'despesa_linhas_correntes_intra': dados_despesa['linhas_correntes_intra'],
            'despesa_total_capital_intra': dados_despesa['total_capital_intra'],
            'despesa_linhas_capital_intra': dados_despesa['linhas_capital_intra'],
            'despesa_total_despesas_intra': dados_despesa['total_despesas_intra'],
            
            # Informações gerais
            'ano': self.ano,
            'bimestre': self.bimestre
        }