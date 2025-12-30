# Arquivo: src/ticker_service.py
import pandas as pd
import yfinance as yf
import requests
import json
from base64 import b64encode
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from curl_cffi import requests as curl_requests
import streamlit as st # Apenas para o cache

# Importa o motor de baixo nível que já criamos
from src import b3_engine

URL_EMPRESAS = "https://github.com/tovarich86/ticker/raw/refs/heads/main/empresas_b3%20(6).xlsx"

@st.cache_data(ttl=3600)
def carregar_empresas():
    """Baixa e trata a planilha de empresas listadas."""
    try:
        df = pd.read_excel(URL_EMPRESAS)
        # Tratamento de strings
        cols = ['Nome do Pregão', 'Tickers', 'CODE', 'typeStock']
        for c in cols:
            if c in df.columns:
                df[c] = df[c].astype(str).fillna('').str.strip().str.upper()
        
        # Ajuste específico de S.A.
        if 'Nome do Pregão' in df.columns:
            df['Nome do Pregão'] = df['Nome do Pregão'].str.replace(r'\s*S\.?A\.?/A?', ' S.A.', regex=True)
            
        return df[(df['Tickers'] != '') & (df['Nome do Pregão'] != '')]
    except Exception as e:
        print(f"Erro ao carregar empresas: {e}")
        return pd.DataFrame()

def _get_ticker_info(ticker, df_empresas):
    """Helper para encontrar códigos internos da B3 (CVM/ISIN) pelo Ticker."""
    t_upper = ticker.strip().upper()
    for _, row in df_empresas.iterrows():
        lista = [x.strip() for x in row['Tickers'].split(',')]
        if t_upper in lista:
            return {
                'trading_name': row['Nome do Pregão'], 
                'code': row['CODE'], 
                'type_stock': row['typeStock']
            }
    return None

def buscar_proventos_b3(ticker, tipo, df_empresas, dt_ini, dt_fim):
    """Busca Dividendos ou Bonificações na API da B3 (usando curl_cffi para bypass)."""
    info = _get_ticker_info(ticker, df_empresas)
    if not info: return pd.DataFrame()
    
    session = curl_requests.Session(impersonate="chrome")
    try:
        if tipo == 'Dividendos':
            # Endpoint de Dividendos
            params = {"language": "pt-br", "pageNumber": "1", "pageSize": "50", "tradingName": info['trading_name']}
            p_b64 = b64encode(json.dumps(params).encode()).decode()
            url = f'https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetListedCashDividends/{p_b64}'
            col_data = 'lastDatePriorEx'
            
        elif tipo == 'Bonificacoes':
            # Endpoint de Bonificações
            if not info.get('code'): return pd.DataFrame()
            params = {"issuingCompany": info['code'], "language": "pt-br"}
            p_b64 = b64encode(json.dumps(params).encode()).decode()
            url = f'https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetListedSupplementCompany/{p_b64}'
            col_data = 'lastDatePrior'
        
        # Requisição
        res = session.get(url, timeout=20)
        data_json = res.json()
        
        # Parser específico
        df = pd.DataFrame()
        if tipo == 'Dividendos' and 'results' in data_json:
            df = pd.DataFrame(data_json['results'])
            if not df.empty:
                df['typeStock'] = df['typeStock'].str.strip().str.upper()
                df = df[df['typeStock'] == info['type_stock']]
                
        elif tipo == 'Bonificacoes' and data_json and "stockDividends" in data_json[0]:
            df = pd.DataFrame(data_json[0]["stockDividends"])

        if df.empty: return pd.DataFrame()

        # Filtro de Data
        df['data_ref'] = pd.to_datetime(df[col_data], format='%d/%m/%Y', errors='coerce')
        df = df[(df['data_ref'] >= dt_ini) & (df['data_ref'] <= dt_fim)]
        return df.drop(columns=['data_ref'])

    except Exception as e:
        print(f"Erro proventos ({tipo}): {e}")
        return pd.DataFrame()

def buscar_cotacoes_hibrido(tickers_str, dt_ini_str, dt_fim_str, df_empresas):
    """
    Lógica principal:
    1. Identifica quais tickers são brasileiros (B3).
    2. Para B3: Baixa o ZIP oficial (b3_engine) para pegar OHLC + Volume.
    3. Para B3: Usa Yahoo apenas para pegar o 'Adj Close'.
    4. Para Gringos: Usa Yahoo para tudo.
    """
    tickers = [t.strip().upper() for t in tickers_str.split(',') if t.strip()]
    d_ini = datetime.strptime(dt_ini_str, "%d/%m/%Y").date()
    d_fim = datetime.strptime(dt_fim_str, "%d/%m/%Y").date()
    
    # Separa B3 de Internacional
    todos_b3 = set()
    for row in df_empresas['Tickers'].dropna():
        for t in row.split(','): todos_b3.add(t.strip().upper())
        
    list_b3 = [t for t in tickers if t in todos_b3]
    list_yf = [t for t in tickers if t not in todos_b3]
    
    resultados = {}
    erros = []

    # --- PROCESSAMENTO B3 ---
    if list_b3:
        # 1. Dados Brutos (Engine)
        dias = b3_engine.listar_dias_uteis(d_ini, d_fim)
        frames = []
        with requests.Session() as s:
            with ThreadPoolExecutor(max_workers=5) as pool:
                # Chama o b3_engine que criamos no Passo 2
                futures = [pool.submit(b3_engine.baixar_e_parsear_dia, d, list_b3, s) for d in dias]
                for f in futures:
                    res = f.result()
                    if res is not None: frames.append(res)
        
        if frames:
            df_total = pd.concat(frames)
            
            # 2. Adj Close (Yahoo)
            sa_tickers = [f"{t}.SA" for t in list_b3]
            try:
                yf_data = yf.download(sa_tickers, start=d_ini, end=d_fim + timedelta(days=5), progress=False)['Adj Close']
            except: yf_data = pd.DataFrame()

            # 3. Merge
            for t in list_b3:
                df_t = df_total[df_total['Ticker'] == t].copy()
                if not df_t.empty:
                    df_t['Date'] = pd.to_datetime(df_t['Date'])
                    df_t = df_t.set_index('Date').sort_index()
                    
                    # Tenta casar o Adj Close
                    col_adj = None
                    ticker_sa = f"{t}.SA"
                    if not yf_data.empty:
                        if isinstance(yf_data, pd.Series): col_adj = yf_data
                        elif ticker_sa in yf_data.columns: col_adj = yf_data[ticker_sa]
                    
                    if col_adj is not None:
                        # Reindexa para garantir alinhamento de datas
                        df_t['Adj Close'] = col_adj.reindex(df_t.index)
                    else:
                        df_t['Adj Close'] = float('nan')
                    
                    resultados[t] = df_t.reset_index()
        else:
            erros.append("B3: Nenhum dado encontrado nos boletins oficiais para o período.")

    # --- PROCESSAMENTO YAHOO (Internacional) ---
    if list_yf:
        try:
            df_yf = yf.download(list_yf, start=d_ini, end=d_fim + timedelta(days=1), progress=False)
            if not df_yf.empty:
                # Tratamento para MultiIndex do Yahoo novo
                if isinstance(df_yf.columns, pd.MultiIndex):
                    # Se tiver mais de um ticker, o nível 1 é o ticker
                    # Lógica simplificada: itera sobre os tickers pedidos
                    for t in list_yf:
                        try:
                            # Tenta extrair cross-section
                            df_t = df_yf.xs(t, axis=1, level=1).copy()
                            df_t['Ticker'] = t
                            resultados[t] = df_t.reset_index()
                        except: pass
                else:
                    # Apenas 1 ticker
                    t = list_yf[0]
                    df_yf['Ticker'] = t
                    resultados[t] = df_yf.reset_index()
        except Exception as e:
            erros.append(f"Yahoo Error: {e}")

    return resultados, erros
