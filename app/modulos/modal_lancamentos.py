# app/modulos/modal_lancamentos.py
"""
Módulo para gerenciar o modal de lançamentos
Permite visualizar lançamentos detalhados com filtros dinâmicos
"""

import sqlite3
from typing import Dict, List, Optional, Any
from flask import jsonify
from app.modulos.formatacao import formatar_moeda


class ModalLancamentos:
    """Gerencia a exibição de lançamentos em modal"""
    
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        
    def buscar_lancamentos(self, filtros: Dict[str, Any]) -> List[Dict]:
        """
        Busca lançamentos com base nos filtros fornecidos
        
        Args:
            filtros: Dicionário com os filtros a aplicar
                - ano: Ano de exercício
                - mes: Mês limite (até)
                - coug: Código da unidade gestora (opcional)
                - cat_id: ID da categoria
                - fonte_id: ID da fonte
                - subfonte_id: ID da subfonte
                - alinea_id: ID da alínea
                - coalinea: Código da alínea (para relatório por fonte)
                - cofonte: Código da fonte (para relatório por fonte)
                
        Returns:
            Lista de lançamentos encontrados
        """
        # Monta a query base
        query = """
            SELECT 
                COUG,
                COCONTACONTABIL,
                NUDOCUMENTO,
                COEVENTO,
                INDEBITOCREDITO,
                VALANCAMENTO
            FROM lancamentos_db.lancamentos
            WHERE COEXERCICIO = ?
              AND INMES <= ?
        """
        
        params = [filtros['ano'], filtros['mes']]
        
        # Adiciona filtros opcionais
        if filtros.get('cat_id'):
            query += " AND CATEGORIARECEITA = ?"
            params.append(filtros['cat_id'])
            
        if filtros.get('fonte_id'):
            query += " AND COFONTERECEITA = ?"
            params.append(filtros['fonte_id'])
            
        if filtros.get('subfonte_id'):
            query += " AND COSUBFONTERECEITA = ?"
            params.append(filtros['subfonte_id'])
            
        if filtros.get('alinea_id'):
            query += " AND COALINEA = ?"
            params.append(filtros['alinea_id'])
            
        # Filtros específicos para relatório por fonte
        if filtros.get('coalinea'):
            query += " AND COALINEA = ?"
            params.append(filtros['coalinea'])
            
        if filtros.get('cofonte'):
            query += " AND COFONTERECEITA = ?"
            params.append(filtros['cofonte'])
            
        # Filtro de conta contábil para receitas
        query += " AND COCONTACONTABIL BETWEEN '621200000' AND '621399999'"
        
        # Filtro de documento do ano
        query += f" AND NUDOCUMENTO LIKE '{filtros['ano']}%'"
        
        # Filtro de COUG se fornecido
        if filtros.get('coug'):
            query += " AND COUGCONTAB = ?"
            params.append(filtros['coug'])
            
        query += " ORDER BY COEVENTO, VALANCAMENTO DESC"
        
        try:
            cursor = self.conn.execute(query, params)
            lancamentos = []
            
            for row in cursor:
                lancamentos.append({
                    'COUG': row['COUG'],
                    'COCONTACONTABIL': row['COCONTACONTABIL'],
                    'NUDOCUMENTO': row['NUDOCUMENTO'],
                    'COEVENTO': row['COEVENTO'],
                    'INDEBITOCREDITO': row['INDEBITOCREDITO'],
                    'VALANCAMENTO': row['VALANCAMENTO']
                })
                
            return lancamentos
            
        except Exception as e:
            print(f"Erro ao buscar lançamentos: {e}")
            return []
    
    def calcular_total_liquido(self, lancamentos: List[Dict]) -> float:
        """
        Calcula o total líquido dos lançamentos
        
        Args:
            lancamentos: Lista de lançamentos
            
        Returns:
            Total líquido (créditos - débitos)
        """
        total = 0.0
        
        for lancamento in lancamentos:
            dc = lancamento.get('INDEBITOCREDITO', '')
            valor = float(lancamento.get('VALANCAMENTO', 0))
            
            if dc == 'C':
                total += valor
            elif dc == 'D':
                total -= valor
                
        return total
    
    def formatar_lancamentos_para_modal(self, lancamentos: List[Dict], 
                                      valor_relatorio: Optional[float] = None) -> Dict:
        """
        Formata os lançamentos para exibição no modal
        
        Args:
            lancamentos: Lista de lançamentos
            valor_relatorio: Valor apurado no relatório (opcional)
            
        Returns:
            Dicionário com dados formatados para o modal
        """
        if not lancamentos:
            return {
                'tem_dados': False,
                'mensagem': 'Nenhum lançamento encontrado para este item com os filtros aplicados.'
            }
        
        # Calcula o total
        total_liquido = self.calcular_total_liquido(lancamentos)
        
        # Formata cada lançamento
        lancamentos_formatados = []
        for lanc in lancamentos:
            lancamentos_formatados.append({
                'coug': lanc['COUG'] or '',
                'conta_contabil': lanc['COCONTACONTABIL'] or '',
                'documento': lanc['NUDOCUMENTO'] or '',
                'evento': lanc['COEVENTO'] or '',
                'dc': lanc['INDEBITOCREDITO'] or '',
                'valor': lanc['VALANCAMENTO'],
                'valor_formatado': formatar_moeda(lanc['VALANCAMENTO'])
            })
        
        return {
            'tem_dados': True,
            'lancamentos': lancamentos_formatados,
            'total_liquido': total_liquido,
            'total_liquido_formatado': formatar_moeda(total_liquido),
            'quantidade': len(lancamentos),
            'valor_relatorio': valor_relatorio,
            'valor_relatorio_formatado': formatar_moeda(valor_relatorio) if valor_relatorio else None
        }
    
    def gerar_html_tabela(self, dados_formatados: Dict) -> str:
        """
        Gera o HTML da tabela de lançamentos
        
        Args:
            dados_formatados: Dados formatados pelo método formatar_lancamentos_para_modal
            
        Returns:
            String HTML da tabela
        """
        if not dados_formatados['tem_dados']:
            return f'<p>{dados_formatados["mensagem"]}</p>'
        
        html = '<div class="modal-info-container">'
        
        # Valor apurado no relatório (apenas se fornecido)
        if dados_formatados.get('valor_relatorio_formatado'):
            html += f'''
                <div class="valor-apurado-info">
                    <strong>Valor Apurado no Relatório:</strong> {dados_formatados['valor_relatorio_formatado']}
                </div>
            '''
        
        html += '</div>'
        
        # Tabela de lançamentos
        html += '''
        <div class="table-container">
            <table class="lancamentos-table">
                <colgroup>
                    <col class="col-conta"><col class="col-ug"><col class="col-doc">
                    <col class="col-evento"><col class="col-dc"><col class="col-valor">
                </colgroup>
                <thead>
                    <tr>
                        <th>Conta Contábil</th><th>UG Emitente</th><th>Documento</th>
                        <th>Evento</th><th>D/C</th><th>Valor</th>
                    </tr>
                </thead>
                <tbody>
        '''
        
        for lanc in dados_formatados['lancamentos']:
            html += f'''
                <tr>
                    <td>{lanc['conta_contabil']}</td>
                    <td>{lanc['coug']}</td>
                    <td>{lanc['documento']}</td>
                    <td>{lanc['evento']}</td>
                    <td>{lanc['dc']}</td>
                    <td>{lanc['valor_formatado']}</td>
                </tr>
            '''
        
        html += f'''
                </tbody>
                <tfoot>
                    <tr>
                        <td colspan="5">Total Líquido dos Lançamentos:</td>
                        <td>{dados_formatados['total_liquido_formatado']}</td>
                    </tr>
                </tfoot>
            </table>
        </div>
        '''
        
        return html


# Funções auxiliares para facilitar o uso

def processar_requisicao_lancamentos(conn: sqlite3.Connection, request_args: dict) -> Dict:
    """
    Processa uma requisição de lançamentos e retorna os dados formatados
    
    Args:
        conn: Conexão com o banco de dados
        request_args: Argumentos da requisição (request.args)
        
    Returns:
        Dicionário com os lançamentos formatados ou erro
    """
    try:
        # Extrai os filtros da requisição
        filtros = {
            'ano': request_args.get('ano', type=int),
            'mes': request_args.get('mes', type=int),
            'coug': request_args.get('coug', ''),
            'cat_id': request_args.get('cat_id'),
            'fonte_id': request_args.get('fonte_id'),
            'subfonte_id': request_args.get('subfonte_id'),
            'alinea_id': request_args.get('alinea_id'),
            'coalinea': request_args.get('coalinea'),
            'cofonte': request_args.get('cofonte')
        }
        
        # Valida filtros obrigatórios
        if not filtros['ano'] or not filtros['mes']:
            return {'erro': 'Ano e mês são obrigatórios'}
        
        # Cria instância do modal
        modal = ModalLancamentos(conn)
        
        # Busca os lançamentos
        lancamentos = modal.buscar_lancamentos(filtros)
        
        # Pega o valor do relatório se fornecido
        valor_relatorio = request_args.get('valor_relatorio', type=float)
        
        # Formata para o modal
        dados_formatados = modal.formatar_lancamentos_para_modal(lancamentos, valor_relatorio)
        
        # Adiciona o HTML da tabela
        dados_formatados['html_tabela'] = modal.gerar_html_tabela(dados_formatados)
        
        return dados_formatados
        
    except Exception as e:
        return {'erro': str(e)}


def gerar_botao_lancamentos(tem_lancamentos: bool, coug_selecionada: str, 
                           params_lancamentos: Dict, nivel: int = 3) -> str:
    """
    Gera o HTML do botão de lançamentos se aplicável
    
    Args:
        tem_lancamentos: Se o item tem lançamentos
        coug_selecionada: COUG selecionada
        params_lancamentos: Parâmetros para buscar os lançamentos
        nivel: Nível do item (padrão 3 para alíneas)
        
    Returns:
        HTML do botão ou string vazia
    """
    if not tem_lancamentos or not coug_selecionada or nivel != 3:
        return ""
    
    import json
    params_json = json.dumps(params_lancamentos)
    
    return f'''
    <button class="btn-lancamentos"
            onclick="abrirModalLancamentos(this)"
            data-params='{params_json}'>
        Lançamentos
    </button>
    '''