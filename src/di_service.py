import sys
import os
import base64
import json
import pandas as pd
from bs4 import BeautifulSoup
from datetime import date, datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from curl_cffi import requests as curl_requests

# Garante a importação do motor da B3 que já existe no seu projeto
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src import b3_engine

# Mapeamento oficial de meses da B3 para contratos futuros
CODIGOS_MES_DI = {
    1: 'F', 2: 'G', 3: 'H', 4: 'J', 5: 'K', 6: 'M',
    7: 'N', 8: 'Q', 9: 'U', 10: 'V', 11: 'X', 12: 'Z'
}

MESES_DI_INV = {v: k for k, v in CODIGOS_MES_DI.items()}

def gerar_opcoes_tickers(data_ref, meses_curtos=12, anos_longos=10):
    """
    Gera dinamicamente as opções de tickers de vencimentos mais próximos e longos.
    Isso alimentará o multiselect na interface do utilizador.
    """
    tickers = []
    
    # Se vier datetime, converte para date
    if isinstance(data_ref, datetime):
        data_ref = data_ref.date()
        
    mes_atual = data_ref.month
    ano_atual = data_ref.year
    
    # 1. Gera os vencimentos curtos (próximos meses)
    for i in range(1, meses_curtos + 1):
        mes_venc = mes_atual + i
        ano_venc = ano_atual
        if mes_venc > 12:
            ano_venc += (mes_venc - 1) // 12
            mes_venc = ((mes_venc - 1) % 12) + 1
            
        tickers.append(f"DI1{CODIGOS_MES_DI[mes_venc]}{str(ano_venc)[-2:]}")
        
    # 2. Gera os vencimentos longos (Janeiro dos próximos anos)
    for i in range(1, anos_longos + 1):
        ano_longo = ano_atual + i
        ticker_longo = f"DI1F{str(ano_longo)[-2:]}"
        if ticker_longo not in tickers:
            tickers.append(ticker_longo)
            
    return tickers

def calcular_dias_uteis_di(ticker, data_ref):
    """
    Calcula os dias úteis entre a data de referência e o vencimento do DI,
    utilizando as regras de feriados do b3_engine.
    """
    if isinstance(data_ref, datetime):
        data_ref = data_ref.date()
        
    letra_mes = ticker[3]
    ano_venc = int("20" + ticker[4:6])
    mes_venc = MESES_DI_INV[letra_mes]
    
    # O Vencimento do DI é sempre no 1º dia útil do mês de vencimento
    dia_teste = date(ano_venc, mes_venc, 1)
    
    # Encontra o 1º dia útil usando a lógica do b3_engine
    while not b3_engine.listar_dias_uteis(dia_teste, dia_teste):
        dia_teste += timedelta(days=1)
        
    data_vencimento = dia_teste
    
    # Se o vencimento já passou em relação à data solicitada, retorna 0
    if data_vencimento <= data_ref:
        return 0
        
    # Conta os dias úteis entre a data de referência e o vencimento
    lista_dias = b3_engine.listar_dias_uteis(data_ref, data_vencimento)
    dias_uteis = max(0, len(lista_dias) - 1) # Desconta o dia de hoje
    
    return dias_uteis

def consultar_taxas_di_advfn(ticker):
    """
    Extrai o histórico lendo o JSON embutido e codificado em Base64 
    direto do HTML do ADVFN.
    """
    url = f"https://br.advfn.com/bolsa-de-valores/bmf/{ticker}/historico"
    session = curl_requests.Session(impersonate="chrome")

    try:
        response = session.get(url, timeout=15)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Encontra a div que contém o payload com os dados da tabela
        table_div = soup.find('div', id='table_more_historical')
        if not table_div:
            return None, "Div de histórico não encontrada no HTML."
            
        data_options_b64 = table_div.get('data-options')
        if not data_options_b64:
            return None, "Atributo data-options ausente no HTML."
            
        # Decodifica Base64 para String JSON e transforma em dicionário
        json_str = base64.b64decode(data_options_b64).decode('utf-8')
        data_json = json.loads(json_str)
        
        records = data_json.get('data', [])
        if not records:
            return None, "A lista de dados retornou vazia."
            
        df = pd.DataFrame(records)
        
        # O Timestamp vem em segundos ("Date": "1774044000"). Convertendo para data local:
        df['DATA_DT'] = pd.to_datetime(df['Date'].astype(int), unit='s')
        df['DATA_DT'] = df['DATA_DT'].dt.tz_localize('UTC').dt.tz_convert('America/Sao_Paulo').dt.date
        
        # Extrai apenas a taxa de fechamento como float real
        df['TAXA'] = df['ClosePrice'].astype(float)
        
        return df[['DATA_DT', 'TAXA']], None

    except Exception as e:
        return None, f"Falha na extração: {str(e)}"

def _processar_ticker_unico(ticker, data_ref):
    """
    Worker paralelo que procura o JSON, filtra a data e calcula dias úteis.
    """
    df_hist, err = consultar_taxas_di_advfn(ticker)
    
    if df_hist is not None and not df_hist.empty:
        # Garante que data_ref é um objeto do tipo date para a comparação
        if isinstance(data_ref, datetime):
            data_ref_date = data_ref.date()
        else:
            data_ref_date = data_ref
            
        # Filtra a linha da data específica
        df_dia = df_hist[df_hist['DATA_DT'] == data_ref_date]
        
        if not df_dia.empty:
            taxa_fechamento = df_dia.iloc[0]['TAXA']
            dias_uteis = calcular_dias_uteis_di(ticker, data_ref_date)
            
            if dias_uteis > 0:
                return {
                    'VENCIMENTO': ticker,
                    'DIAS_UTEIS': dias_uteis,
                    'TAXA (%)': taxa_fechamento
                }
            
    return None

def consultar_taxas_di_por_tickers(data_ref, tickers_selecionados):
    """
    Função principal: Procura apenas os tickers que o utilizador selecionou.
    Executa a extração de forma paralela para acelerar o retorno.
    """
    if not tickers_selecionados:
        return None, "Nenhum ticker selecionado."

    resultados = []
    erros_execucao = []
    
    # Limita o número de ligações simultâneas para não sobrecarregar
    max_workers = min(len(tickers_selecionados), 8)
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_processar_ticker_unico, t, data_ref): t for t in tickers_selecionados}
        
        for future in futures:
            try:
                resultado = future.result()
                if resultado:
                    resultados.append(resultado)
            except Exception as e:
                erros_execucao.append(f"Erro no {futures[future]}: {str(e)}")
                
    if not resultados:
        return None, "Nenhum dado encontrado para os tickers na data solicitada (verifique se é feriado/fim de semana)."
        
    df_final = pd.DataFrame(resultados)
    
    # Ordena a curva logicamente pelos dias úteis crescentes
    df_final = df_final.sort_values(by='DIAS_UTEIS').reset_index(drop=True)
    
    return df_final, None
