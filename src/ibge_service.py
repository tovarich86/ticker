# Arquivo: src/ibge_service.py
import requests
import pandas as pd
from datetime import datetime, timedelta

def carregar_dados_ipca():
    """Baixa a série histórica do IPCA diretamente do SIDRA/IBGE."""
    url = "https://apisidra.ibge.gov.br/values/t/1737/n1/all/v/all/p/all/d/v63%202,v69%202,v2266%2013,v2263%202,v2264%202,v2265%202?formato=json"
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        dados_json = response.json()
        
        df = pd.DataFrame(dados_json[1:])
        df = df[df['D2C'] == '2266'] # Filtra o índice geral
        
        # Converte a data "YYYYMM" para datetime
        def _parse_data(codigo):
            c = str(codigo)
            return datetime(int(c[:4]), int(c[4:6]), 1)
            
        df['data'] = df['D3C'].apply(_parse_data)
        df['valor'] = pd.to_numeric(df['V'], errors='coerce')
        df = df.sort_values('data').reset_index(drop=True)
        return df
    except Exception as e:
        print(f"Erro ao baixar IPCA: {e}")
        return pd.DataFrame()

def calcular_correcao_ipca(df_ipca, data_inicial, data_final, valor_inicial):
    """Realiza o cálculo de correção monetária entre duas datas."""
    # Busca índice do mês ANTERIOR ao inicial (base do cálculo)
    mes_anterior = data_inicial - timedelta(days=1)
    mes_anterior = datetime(mes_anterior.year, mes_anterior.month, 1)
    
    busca_indice = df_ipca[df_ipca['data'] == mes_anterior]['valor']
    
    if busca_indice.empty:
        return None, None, None, None # Dados insuficientes
        
    indice_base = busca_indice.values[0]
    
    # Filtra o período
    df_periodo = df_ipca[(df_ipca['data'] >= data_inicial) & (df_ipca['data'] <= data_final)].copy()
    
    if df_periodo.empty:
        return None, None, None, None
        
    df_periodo = df_periodo.sort_values('data').reset_index(drop=True)
    
    # Calcula variação mensal
    df_periodo['var_mes'] = df_periodo['valor'].pct_change()
    # Ajuste do primeiro mês em relação à base
    df_periodo.loc[0, 'var_mes'] = (df_periodo.loc[0, 'valor'] / indice_base) - 1
    
    # Resultado acumulado
    ipca_acumulado = (df_periodo.iloc[-1]['valor'] / indice_base) - 1
    valor_corrigido = valor_inicial * (1 + ipca_acumulado)
    
    return ipca_acumulado, df_periodo, valor_corrigido, indice_base
