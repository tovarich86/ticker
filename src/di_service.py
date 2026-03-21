import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# Mapeamento oficial de meses da B3 para contratos futuros
CODIGOS_MES_DI = {
    1: 'F', 2: 'G', 3: 'H', 4: 'J', 5: 'K', 6: 'M',
    7: 'N', 8: 'Q', 9: 'U', 10: 'V', 11: 'X', 12: 'Z'
}

def gerar_tickers_di(data_ref, meses_curtos=6, anos_longos=5):
    """
    Gera dinamicamente os tickers dos vencimentos mais próximos.
    - meses_curtos: Quantos vencimentos mensais sequenciais buscar.
    - anos_longos: Quantos vencimentos anuais (sempre Janeiro - F) buscar para o longo prazo.
    """
    tickers = []
    mes_atual = data_ref.month
    ano_atual = data_ref.year
    
    # 1. Gera os vencimentos curtos (ex: próximos 6 meses)
    for i in range(1, meses_curtos + 1):
        mes_venc = mes_atual + i
        ano_venc = ano_atual
        if mes_venc > 12:
            ano_venc += (mes_venc - 1) // 12
            mes_venc = ((mes_venc - 1) % 12) + 1
        
        # Formato: DI1 + Letra do Mês + Ano (2 últimos dígitos)
        tickers.append(f"DI1{CODIGOS_MES_DI[mes_venc]}{str(ano_venc)[-2:]}")
        
    # 2. Gera os vencimentos longos (Janeiro dos próximos anos)
    for i in range(1, anos_longos + 1):
        ano_longo = ano_atual + i
        ticker_longo = f"DI1F{str(ano_longo)[-2:]}"
        if ticker_longo not in tickers:
            tickers.append(ticker_longo)
            
    return tickers

def consultar_taxas_di_advfn(ticker):
    """Faz o web scraping do histórico de um ticker específico no ADVFN."""
    url = f"https://br.advfn.com/bolsa-de-valores/bmf/{ticker}/historico"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
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
        
        if not data_list: return None, "Sem dados"

        df = pd.DataFrame(data_list)
        # Manteremos apenas a Data e o Fechamento (ClosePrice)
        df = df[['Date', 'ClosePrice']].rename(columns={'Date': 'DATA', 'ClosePrice': 'TAXA'})
        
        # Converte a taxa de string brasileira para float (ex: 14,42 -> 14.42)
        df['TAXA'] = df['TAXA'].str.replace('.', '', regex=False).str.replace(',', '.', regex=False).astype(float)
        
        return df, None

    except Exception as e:
        return None, str(e)

def _processar_ticker_unico(ticker, data_ref):
    """Função auxiliar para rodar em paralelo."""
    df_hist, err = consultar_taxas_di_advfn(ticker)
    if df_hist is not None and not df_hist.empty:
        # Tenta converter a string de data do ADVFN para datetime
        df_hist['DATA_DT'] = pd.to_datetime(df_hist['DATA'], errors='coerce')
        
        # Filtra pela data exata que o usuário pediu no Streamlit
        df_dia = df_hist[df_hist['DATA_DT'].dt.date == data_ref]
        
        if not df_dia.empty:
            taxa_fechamento = df_dia.iloc[0]['TAXA']
            return {'VENCIMENTO': ticker, 'TAXA_FECHAMENTO (%)': taxa_fechamento}
            
    return None

def consultar_taxas_di(data_ref):
    """
    Função principal chamada pela interface 'pages/02_📉_Taxas_DI.py'.
    Gera a curva de juros para a data informada.
    """
    # 1. Gera os tickers mais próximos baseados na data selecionada
    tickers = gerar_tickers_di(data_ref, meses_curtos=6, anos_longos=5)
    
    resultados = []
    
    # 2. Usa ThreadPoolExecutor para baixar todos os tickers do ADVFN ao mesmo tempo
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(_processar_ticker_unico, t, data_ref) for t in tickers]
        
        for future in futures:
            resultado = future.result()
            if resultado:
                resultados.append(resultado)
                
    # 3. Formata a saída
    if not resultados:
        return None, "Nenhum dado encontrado para a data solicitada (Verifique se é um dia útil)."
        
    df_final = pd.DataFrame(resultados)
    
    # Opcional: Ordenar a curva por nome do vencimento
    df_final = df_final.sort_values(by='VENCIMENTO').reset_index(drop=True)
    
    return df_final, None
