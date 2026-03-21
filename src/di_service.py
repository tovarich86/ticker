import sys
import os
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

def gerar_tickers_di(data_ref, meses_curtos=8, anos_longos=6):
    """
    Gera dinamicamente os tickers dos vencimentos mais próximos.
    - meses_curtos: Quantos vencimentos mensais sequenciais buscar.
    - anos_longos: Quantos vencimentos anuais (sempre Janeiro - F) buscar para o longo prazo.
    """
    tickers = []
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
    utilizando as regras de feriados do seu b3_engine.
    """
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
    Faz o web scraping do histórico de um ticker específico no ADVFN.
    Utiliza curl_cffi (impersonate="chrome") para evitar bloqueios.
    """
    url = f"https://br.advfn.com/bolsa-de-valores/bmf/{ticker}/historico"
    
    # Usando a mesma técnica anti-bloqueio já utilizada no seu ticker_service.py
    session = curl_requests.Session(impersonate="chrome")

    try:
        response = session.get(url, timeout=15)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        rows = soup.find_all('div', {'role': 'row'})
        
        data_list = []
        for row in rows:
            cells = row.find_all('div', {'role': 'gridcell'})
            if not cells: continue
                
            row_data = {}
            for cell in cells:
                col_id = cell.get('col-id')
                val_span = cell.find('span', {'class': 'ag-cell-value'})
                if col_id and val_span:
                    row_data[col_id] = val_span.get_text(strip=True)
            
            if row_data:
                data_list.append(row_data)
        
        if not data_list: return None, "Sem dados na tabela"

        df = pd.DataFrame(data_list)
        df = df[['Date', 'ClosePrice']].rename(columns={'Date': 'DATA', 'ClosePrice': 'TAXA'})
        
        # Converte a taxa de string (14,42) para float (14.42)
        df['TAXA'] = df['TAXA'].str.replace('.', '', regex=False).str.replace(',', '.', regex=False).astype(float)
        
        return df, None

    except Exception as e:
        return None, str(e)

def _processar_ticker_unico(ticker, data_ref):
    """
    Função auxiliar para consultar o ADVFN, filtrar pela data desejada
    e calcular os dias úteis. Projetada para execução paralela.
    """
    df_hist, err = consultar_taxas_di_advfn(ticker)
    
    if df_hist is not None and not df_hist.empty:
        # Tenta converter a string de data (ex: '20 Mar 2026') para datetime
        df_hist['DATA_DT'] = pd.to_datetime(df_hist['DATA'], errors='coerce')
        
        # Filtra pela data exata solicitada pelo usuário
        df_dia = df_hist[df_hist['DATA_DT'].dt.date == data_ref]
        
        if not df_dia.empty:
            taxa_fechamento = df_dia.iloc[0]['TAXA']
            dias_uteis = calcular_dias_uteis_di(ticker, data_ref)
            
            # Só retorna se for um vencimento válido (dias úteis > 0)
            if dias_uteis > 0:
                return {
                    'VENCIMENTO': ticker,
                    'DIAS_UTEIS': dias_uteis,
                    'TAXA (%)': taxa_fechamento
                }
            
    return None

def consultar_taxas_di(data_ref):
    """
    Função principal chamada pela interface do Streamlit.
    Retorna o DataFrame final montado com a curva DI.
    """
    tickers = gerar_tickers_di(data_ref, meses_curtos=8, anos_longos=6)
    resultados = []
    erros_execucao = []
    
    # Dispara a busca simultânea para acelerar o processo
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(_processar_ticker_unico, t, data_ref): t for t in tickers}
        
        for future in futures:
            try:
                resultado = future.result()
                if resultado:
                    resultados.append(resultado)
            except Exception as e:
                erros_execucao.append(f"Erro no {futures[future]}: {str(e)}")
                
    if not resultados:
        return None, "Nenhum dado encontrado. A data solicitada pode ser feriado/final de semana ou muito distante no passado."
        
    df_final = pd.DataFrame(resultados)
    
    # Ordena a curva logicamente pelos dias úteis crescentes
    df_final = df_final.sort_values(by='DIAS_UTEIS').reset_index(drop=True)
    
    return df_final, None
