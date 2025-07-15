# app/modulos/formatacao.py
"""Formatação de valores monetários e numéricos no padrão brasileiro"""

import locale
from decimal import Decimal

# Tenta configurar o locale para pt_BR
try:
    locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
except:
    try:
        locale.setlocale(locale.LC_ALL, 'Portuguese_Brazil.1252')
    except:
        # Se não conseguir, usaremos formatação manual
        pass

def formatar_moeda(valor, prefixo="R$", usar_cor=False, html=False):
    """
    Formata valor monetário no padrão brasileiro
    
    Args:
        valor: Valor numérico a ser formatado
        prefixo: Prefixo monetário (padrão: R$)
        usar_cor: Se True, retorna com código de cor
        html: Se True, retorna com tags HTML para cor
    
    Returns:
        String formatada
    """
    if valor is None:
        return f"{prefixo} 0,00"
    
    # Converte para float se necessário
    if isinstance(valor, str):
        valor = float(valor.replace(',', '.'))
    
    # Determina se é negativo
    negativo = valor < 0
    valor_abs = abs(valor)
    
    # Formata o valor
    try:
        # Tenta usar o locale
        valor_formatado = locale.currency(valor_abs, symbol=False, grouping=True)
    except:
        # Formatação manual se locale não funcionar
        valor_formatado = f"{valor_abs:,.2f}"
        # Substitui separadores para padrão brasileiro
        valor_formatado = valor_formatado.replace(',', '_').replace('.', ',').replace('_', '.')
    
    # Adiciona prefixo
    if negativo:
        valor_final = f"({prefixo} {valor_formatado})"
    else:
        valor_final = f"{prefixo} {valor_formatado}"
    
    # Adiciona cor se solicitado
    if usar_cor:
        if html:
            if negativo:
                return f'<span style="color: red;">{valor_final}</span>'
            elif valor > 0:
                return f'<span style="color: green;">{valor_final}</span>'
            else:
                return valor_final
        else:
            # Para terminal (ANSI colors)
            if negativo:
                return f'\033[91m{valor_final}\033[0m'  # Vermelho
            elif valor > 0:
                return f'\033[92m{valor_final}\033[0m'  # Verde
            else:
                return valor_final
    
    return valor_final

def formatar_percentual(valor, casas_decimais=2, usar_cor=False, html=False):
    """
    Formata valor percentual
    
    Args:
        valor: Valor percentual (ex: 0.15 para 15%)
        casas_decimais: Número de casas decimais (padrão: 2)
        usar_cor: Se True, retorna com código de cor
        html: Se True, retorna com tags HTML para cor
    """
    if valor is None:
        return "0,00%"
    
    # Multiplica por 100 para percentual
    percentual = valor * 100
    
    # Formata
    formato = f"{{:.{casas_decimais}f}}%"
    valor_formatado = formato.format(percentual).replace('.', ',')
    
    # Adiciona sinal de + para valores positivos
    if percentual > 0:
        valor_formatado = f"+{valor_formatado}"
    
    # Adiciona cor se solicitado
    if usar_cor:
        if html:
            if percentual < 0:
                return f'<span style="color: red;">{valor_formatado}</span>'
            elif percentual > 0:
                return f'<span style="color: green;">{valor_formatado}</span>'
            else:
                return valor_formatado
        else:
            # Para terminal
            if percentual < 0:
                return f'\033[91m{valor_formatado}\033[0m'  # Vermelho
            elif percentual > 0:
                return f'\033[92m{valor_formatado}\033[0m'  # Verde
            else:
                return valor_formatado
    
    return valor_formatado

def formatar_numero(valor, casas_decimais=0):
    """
    Formata número no padrão brasileiro (ponto como separador de milhar)
    
    Args:
        valor: Valor numérico
        casas_decimais: Número de casas decimais
    """
    if valor is None:
        return "0"
    
    if casas_decimais > 0:
        formato = f"{{:,.{casas_decimais}f}}"
        valor_formatado = formato.format(valor)
    else:
        valor_formatado = f"{int(valor):,}"
    
    # Substitui para padrão brasileiro
    return valor_formatado.replace(',', '_').replace('.', ',').replace('_', '.')

def formatar_resumo_financeiro(dados, usar_cor=True, html=False):
    """
    Formata um dicionário de dados financeiros
    
    Args:
        dados: Dicionário com valores financeiros
        usar_cor: Se True, aplica cores aos valores
        html: Se True, usa formatação HTML
    
    Returns:
        Dicionário com valores formatados
    """
    resultado = {}
    for chave, valor in dados.items():
        if 'percentual' in chave.lower() or 'crescimento' in chave.lower():
            resultado[chave] = formatar_percentual(valor, usar_cor=usar_cor, html=html)
        elif isinstance(valor, (int, float)):
            resultado[chave] = formatar_moeda(valor, usar_cor=usar_cor, html=html)
        else:
            resultado[chave] = valor
    
    return resultado

# Classe para uso em templates Jinja2
class FormatadorMonetario:
    """Classe helper para usar em templates"""
    
    @staticmethod
    def moeda(valor):
        return formatar_moeda(valor)
    
    @staticmethod
    def moeda_cor_html(valor):
        return formatar_moeda(valor, usar_cor=True, html=True)
    
    @staticmethod
    def percentual(valor):
        return formatar_percentual(valor)
    
    @staticmethod
    def percentual_cor_html(valor):
        return formatar_percentual(valor, usar_cor=True, html=True)
    
    @staticmethod
    def numero(valor, casas=0):
        return formatar_numero(valor, casas)

# TESTE
if __name__ == "__main__":
    print("=== TESTE DE FORMATAÇÃO DE VALORES ===")
    print()
    
    # Valores de teste
    valores_teste = [
        22982101363.93,
        -2056212933.67,
        0,
        1500.50,
        -750.25,
        1000000
    ]
    
    print("📊 Formatação de Moeda:")
    print("-" * 50)
    for valor in valores_teste:
        print(f"{valor:>20} → {formatar_moeda(valor)}")
    
    print("\n📊 Formatação de Moeda com Cores (Terminal):")
    print("-" * 50)
    for valor in valores_teste:
        print(f"{valor:>20} → {formatar_moeda(valor, usar_cor=True)}")
    
    print("\n📊 Formatação de Moeda com Cores (HTML):")
    print("-" * 50)
    for valor in valores_teste:
        print(f"{valor:>20} → {formatar_moeda(valor, usar_cor=True, html=True)}")
    
    # Teste de percentuais
    print("\n\n📈 Formatação de Percentuais:")
    print("-" * 50)
    percentuais = [0.163, -0.163, 0.05, -0.25, 0]
    for valor in percentuais:
        print(f"{valor:>10} → {formatar_percentual(valor, usar_cor=True)}")
    
    # Teste com dados reais
    print("\n\n💰 Exemplo com Dados Reais:")
    print("-" * 50)
    dados = {
        'previsao_inicial': 48535054229.00,
        'deducoes_prev_inicial': -3041426769.00,
        'previsao_inicial_liquida': 45493627460.00,
        'receita_bruta': 25038314297.60,
        'deducoes_receita': -2056212933.67,
        'receita_liquida': 22982101363.93,
        'crescimento_percentual': -0.163
    }
    
    dados_formatados = formatar_resumo_financeiro(dados, usar_cor=True)
    for chave, valor in dados_formatados.items():
        print(f"{chave:.<30} {valor}")