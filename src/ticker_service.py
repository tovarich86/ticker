# Arquivo: src/ticker_service.py
import pandas as pd
import yfinance as yf
import requests
import json
from base64 import b64encode
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from curl_cffi import requests as curl_requests
import streamlit as st 

# Importa o motor de baixo nível que já configuramos
from src import b3_engine

# URL do arquivo no GitHub (Mesma da versão antiga)
URL_EMPRESAS = "import pandas as pd
import yfinance as yf
import requests
import json
from base64 import b64encode
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from curl_cffi import requests as curl_requests
import streamlit as st 

# Importa o motor de baixo nível
from src import b3_engine

# --- CORREÇÃO DA URL (Convertida para formato RAW) ---
# O link original era: https://github.com/tovarich86/ticker/blob/main/assets/empresas_b3.xlsx
# Para funcionar no Pandas, usamos o link 'raw':
URL_EMPRESAS = "https://github.com/tovarich86/ticker/raw/main/assets/empresas_b3 (6).xlsx"

@st.cache_data(ttl=3600)
def carregar_empresas(arquivo_upload=None):
    """
    Carrega a base de empresas da B3.
    """
    df = pd.DataFrame()
    fonte = ""

    try:
        # 1. Tenta carregar do Upload ou do GitHub
        if arquivo_upload:
            fonte = "Arquivo Local"
            df = pd.read_excel(arquivo_upload)
        else:
            fonte = "GitHub"
            # O engine='openpyxl' é recomendado para arquivos .xlsx
            df = pd.read_excel(URL_EMPRESAS, engine='openpyxl')

        # 2. Verificação de Segurança (Evita o KeyError)
        if df.empty:
            return pd.DataFrame()

        # Limpeza dos nomes das colunas
        df.columns = df.columns.str.strip()

        # Se a coluna 'Tickers' não existir, retorna vazio para acionar o fallback na tela
        if 'Tickers' not in df.columns:
            # print(f"Colunas encontradas: {df.columns}") # Debug se necessário
            return pd.DataFrame()

        # 3. Processamento de Dados
        cols_to_process = ['Nome do Pregão', 'Tickers', 'CODE', 'typeStock']
        for col in cols_to_process:
            if col in df.columns:
                df[col] = df[col].astype(str).fillna('').str.strip()
                if col == 'Nome do Pregão':
                    df[col] = df[col].str.replace(r'\s*S\.?A\.?/A?', ' S.A.', regex=True).str.upper()
                if col == 'typeStock':
                    df[col] = df[col].str.upper()
        
        # Filtra linhas válidas
        return df[(df['Tickers'] != '') & (df['Nome do Pregão'] != '')]

    except Exception as e:
        print(f"Erro ao carregar empresas ({fonte}): {e}")
        return pd.DataFrame()

def get_ticker_info(ticker, df_empresas):
    """Busca informações do ticker de forma segura."""
    if df_empresas.empty: return None
    
    ticker_upper = ticker.strip().upper()
    
    for _, row in df_empresas.iterrows():
        val_tickers = str(row['Tickers']) 
        tickers_list = [t.strip().upper() for t in val_tickers.split(",") if t.strip()]
        
        if ticker_upper in tickers_list:
            return {
                'trading_name': row.get('Nome do Pregão', ''), 
                'code': row.get('CODE', ''), 
                'type_stock': row.get('typeStock', '')
            }
    return None

def buscar_dividendos_b3(ticker, empresas_df, data_inicio, data_fim):
    """Busca Dividendos via API B3."""
    if empresas_df.empty: return pd.DataFrame()

    ticker_info = get_ticker_info(ticker, empresas_df)
    if not ticker_info: return pd.DataFrame()
    
    trading_name = ticker_info['trading_name']
    desired_type_stock = ticker_info['type_stock']
    
    session = curl_requests.Session(impersonate="chrome")
    try:
        params = {"language": "pt-br", "pageNumber": "1", "pageSize": "50", "tradingName": trading_name}
        params_encoded = b64encode(json.dumps(params).encode('utf-8')).decode('utf-8')
        url = f'https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetListedCashDividends/{params_encoded}'
        
        response = session.get(url, timeout=20)
        res_json = response.json()
        
        if 'results' in res_json:
            df = pd.DataFrame(res_json['results'])
            if df.empty: return pd.DataFrame()
            
            if 'typeStock' in df.columns:
                df['typeStock'] = df['typeStock'].str.strip().str.upper()
                df = df[df['typeStock'] == desired_type_stock].copy()
            
            df['lastDatePriorEx_dt'] = pd.to_datetime(df['lastDatePriorEx'], format='%d/%m/%Y', errors='coerce')
            df = df[(df['lastDatePriorEx_dt'] >= data_inicio) & (df['lastDatePriorEx_dt'] <= data_fim)]
            return df.drop(columns=['lastDatePriorEx_dt'])
    except Exception: 
        return pd.DataFrame()
    return pd.DataFrame()

def buscar_bonificacoes_b3(ticker, empresas_df, data_inicio, data_fim):
    """Busca Bonificações via API B3."""
    if empresas_df.empty: return pd.DataFrame()

    ticker_info = get_ticker_info(ticker, empresas_df)
    if not ticker_info or not ticker_info.get('code'): return pd.DataFrame()
    
    session = curl_requests.Session(impersonate="chrome")
    try:
        params = {"issuingCompany": ticker_info['code'], "language": "pt-br"}
        params_encoded = b64encode(json.dumps(params).encode('utf-8')).decode('utf-8')
        url = f'https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetListedSupplementCompany/{params_encoded}'
        
        response = session.get(url, timeout=20)
        data = response.json()
        
        if data and "stockDividends" in data[0]:
            df = pd.DataFrame(data[0]["stockDividends"])
            if df.empty: return pd.DataFrame()
            
            df['lastDatePrior_dt'] = pd.to_datetime(df['lastDatePrior'], format='%d/%m/%Y', errors='coerce')
            df = df[(df['lastDatePrior_dt'] >= data_inicio) & (df['lastDatePrior_dt'] <= data_fim)]
            return df.drop(columns=['lastDatePrior_dt'])
    except Exception: 
        return pd.DataFrame()
    return pd.DataFrame()

def buscar_dados_hibrido(tickers_input, dt_ini_str, dt_fim_str, empresas_df):
    """
    Lógica Híbrida: B3 (Engine) + Yahoo.
    """
    if empresas_df.empty:
        return {}, ["Erro: Base de empresas não carregada. Verifique a URL ou faça upload manual."]

    tickers_list = [t.strip().upper() for t in tickers_input.split(',') if t.strip()]
    d_ini = datetime.strptime(dt_ini_str, "%d/%m/%Y").date()
    d_fim = datetime.strptime(dt_fim_str, "%d/%m/%Y").date()
    
    # Identificação B3 vs Internacional
    b3_tickers_set = set()
    if 'Tickers' in empresas_df.columns:
        for row in empresas_df['Tickers'].dropna().astype(str).str.split(','):
            for t in row: b3_tickers_set.add(t.strip().upper())
    
    list_b3 = [t for t in tickers_list if t in b3_tickers_set]
    list_yf = [t for t in tickers_list if t not in b3_tickers_set]
    
    resultados = {}
    erros = []

    # 1. B3 (Engine + Yahoo Adj Close)
    if list_b3:
        dias_uteis = b3_engine.listar_dias_uteis(d_ini, d_fim)
        frames_b3 = []
        with requests.Session() as session:
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = [executor.submit(b3_engine.baixar_e_parsear_dia, d, list_b3, session) for d in dias_uteis]
                for f in futures:
                    res = f.result()
                    if res is not None: frames_b3.append(res)
        
        if frames_b3:
            df_b3_total = pd.concat(frames_b3)
            
            # Yahoo Adj Close
            sa_tickers = [f"{t}.SA" for t in list_b3]
            adj_data = pd.DataFrame()
            try:
                yf_res = yf.download(sa_tickers, start=d_ini, end=d_fim + timedelta(days=5), progress=False)
                if not yf_res.empty:
                    # Tenta pegar Adj Close, se não tiver, pega Close
                    if 'Adj Close' in yf_res.columns:
                        adj_data = yf_res['Adj Close']
                    elif 'Close' in yf_res.columns:
                        adj_data = yf_res['Close']
            except Exception as e:
                erros.append(f"Aviso Yahoo (Adj): {e}")

            for t in list_b3:
                df_t = df_b3_total[df_b3_total['Ticker'] == t].copy()
                if not df_t.empty:
                    ticker_sa = f"{t}.SA"
                    col_adj = None
                    
                    if not adj_data.empty:
                        if isinstance(adj_data, pd.Series): 
                            col_adj = adj_data
                        elif ticker_sa in adj_data.columns:
                            col_adj = adj_data[ticker_sa]
                    
                    df_t = df_t.set_index(pd.to_datetime(df_t['Date']))
                    df_t['Adj Close'] = col_adj.reindex(df_t.index) if col_adj is not None else float('nan')
                    df_t = df_t.reset_index()
                    df_t['Date'] = df_t['Date'].dt.strftime('%d/%m/%Y')
                    
                    cols = ['Ticker', 'Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
                    # Garante que só seleciona colunas existentes
                    cols_existentes = [c for c in cols if c in df_t.columns]
                    resultados[t] = df_t[cols_existentes]
        else:
            erros.append("B3: Sem dados oficiais (feriado ou falha no download do ZIP).")

    # 2. Yahoo (Internacional)
    if list_yf:
        try:
            dados_yf = yf.download(list_yf, start=d_ini, end=d_fim + timedelta(days=1), progress=False)
            if not dados_yf.empty:
                # Tratamento Yahoo MultiIndex
                if isinstance(dados_yf.columns, pd.MultiIndex):
                    for t in list_yf:
                        try:
                            df_t = dados_yf.xs(t, axis=1, level=1).copy()
                            df_t['Ticker'] = t
                            df_t = df_t.reset_index()
                            df_t['Date'] = df_t['Date'].dt.strftime('%d/%m/%Y')
                            if 'Adj Close' not in df_t.columns: df_t['Adj Close'] = df_t['Close']
                            resultados[t] = df_t
                        except: pass
                else:
                    t = list_yf[0]
                    dados_yf['Ticker'] = t
                    dados_yf = dados_yf.reset_index()
                    dados_yf['Date'] = dados_yf['Date'].dt.strftime('%d/%m/%Y')
                    resultados[t] = dados_yf
        except Exception as e:
            erros.append(f"Erro Yahoo Intl: {e}")

    return resultados, erros"

@st.cache_data(ttl=3600)
def carregar_empresas(arquivo_upload=None):
    """
    Tenta carregar a base de empresas.
    Prioridade: 1. Upload Manual (se houver falha no GitHub) -> 2. Download GitHub.
    """
    df = pd.DataFrame()
    fonte = ""

    try:
        # 1. Tenta carregar do Upload ou do GitHub
        if arquivo_upload:
            fonte = "Arquivo Local"
            df = pd.read_excel(arquivo_upload)
        else:
            fonte = "GitHub"
            # O engine='openpyxl' é mais robusto para xlsx modernos
            df = pd.read_excel(URL_EMPRESAS, engine='openpyxl')

        # 2. Verificação de Segurança (O Pulo do Gato para evitar KeyError)
        if df.empty:
            return pd.DataFrame() # Retorna vazio tratado

        # Limpa espaços nos nomes das colunas (ex: "Tickers " -> "Tickers")
        df.columns = df.columns.str.strip()

        # Se a coluna 'Tickers' não existir, algo está errado com o arquivo
        if 'Tickers' not in df.columns:
            st.error(f"⚠️ Erro: Coluna 'Tickers' não encontrada na fonte {fonte}.")
            return pd.DataFrame()

        # 3. Processamento de Dados (Lógica original da versão antiga)
        cols_to_process = ['Nome do Pregão', 'Tickers', 'CODE', 'typeStock']
        for col in cols_to_process:
            if col in df.columns:
                df[col] = df[col].astype(str).fillna('').str.strip()
                if col == 'Nome do Pregão':
                    df[col] = df[col].str.replace(r'\s*S\.?A\.?/A?', ' S.A.', regex=True).str.upper()
                if col == 'typeStock':
                    df[col] = df[col].str.upper()
        
        # Filtra linhas inválidas
        return df[(df['Tickers'] != '') & (df['Nome do Pregão'] != '')]

    except Exception as e:
        # Não exibe erro na tela imediatamente para permitir o fluxo de fallback na página
        print(f"Erro ao carregar empresas ({fonte}): {e}")
        return pd.DataFrame()

def get_ticker_info(ticker, df_empresas):
    """Busca informações do ticker de forma segura."""
    if df_empresas.empty: return None
    
    ticker_upper = ticker.strip().upper()
    
    # Itera com segurança
    for _, row in df_empresas.iterrows():
        # Garante que é string antes de fazer split
        val_tickers = str(row['Tickers']) 
        tickers_list = [t.strip().upper() for t in val_tickers.split(",") if t.strip()]
        
        if ticker_upper in tickers_list:
            return {
                'trading_name': row.get('Nome do Pregão', ''), 
                'code': row.get('CODE', ''), 
                'type_stock': row.get('typeStock', '')
            }
    return None

def buscar_dividendos_b3(ticker, empresas_df, data_inicio, data_fim):
    """Busca Dividendos mantendo o uso do curl_cffi da versão antiga."""
    if empresas_df.empty: return pd.DataFrame()

    ticker_info = get_ticker_info(ticker, empresas_df)
    if not ticker_info: return pd.DataFrame()
    
    trading_name = ticker_info['trading_name']
    desired_type_stock = ticker_info['type_stock']
    
    # Mantém o impersonate para evitar bloqueio da B3
    session = curl_requests.Session(impersonate="chrome")
    try:
        params = {"language": "pt-br", "pageNumber": "1", "pageSize": "50", "tradingName": trading_name}
        # Codificação correta para a API da B3
        params_encoded = b64encode(json.dumps(params).encode('utf-8')).decode('utf-8')
        url = f'https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetListedCashDividends/{params_encoded}'
        
        response = session.get(url, timeout=20)
        res_json = response.json()
        
        if 'results' in res_json:
            df = pd.DataFrame(res_json['results'])
            if df.empty: return pd.DataFrame()
            
            # Filtro por tipo de ação (ON/PN)
            if 'typeStock' in df.columns:
                df['typeStock'] = df['typeStock'].str.strip().str.upper()
                df = df[df['typeStock'] == desired_type_stock].copy()
            
            df['lastDatePriorEx_dt'] = pd.to_datetime(df['lastDatePriorEx'], format='%d/%m/%Y', errors='coerce')
            df = df[(df['lastDatePriorEx_dt'] >= data_inicio) & (df['lastDatePriorEx_dt'] <= data_fim)]
            return df.drop(columns=['lastDatePriorEx_dt'])
    except Exception: 
        return pd.DataFrame()
    return pd.DataFrame()

def buscar_bonificacoes_b3(ticker, empresas_df, data_inicio, data_fim):
    """Busca Bonificações (Lógica original preservada)."""
    if empresas_df.empty: return pd.DataFrame()

    ticker_info = get_ticker_info(ticker, empresas_df)
    if not ticker_info or not ticker_info.get('code'): return pd.DataFrame()
    
    session = curl_requests.Session(impersonate="chrome")
    try:
        params = {"issuingCompany": ticker_info['code'], "language": "pt-br"}
        params_encoded = b64encode(json.dumps(params).encode('utf-8')).decode('utf-8')
        url = f'https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetListedSupplementCompany/{params_encoded}'
        
        response = session.get(url, timeout=20)
        data = response.json()
        
        if data and "stockDividends" in data[0]:
            df = pd.DataFrame(data[0]["stockDividends"])
            if df.empty: return pd.DataFrame()
            
            df['lastDatePrior_dt'] = pd.to_datetime(df['lastDatePrior'], format='%d/%m/%Y', errors='coerce')
            df = df[(df['lastDatePrior_dt'] >= data_inicio) & (df['lastDatePrior_dt'] <= data_fim)]
            return df.drop(columns=['lastDatePrior_dt'])
    except Exception: 
        return pd.DataFrame()
    return pd.DataFrame()

def buscar_dados_hibrido(tickers_input, dt_ini_str, dt_fim_str, empresas_df):
    """
    Lógica Híbrida Original: B3 (Engine) + Yahoo (Adj Close e Internacionais).
    """
    # Verificação crítica para evitar o KeyError 'Tickers'
    if empresas_df.empty:
        return {}, ["Erro: Base de empresas não carregada. Faça o upload manual na tela."]

    tickers_list = [t.strip().upper() for t in tickers_input.split(',') if t.strip()]
    d_ini = datetime.strptime(dt_ini_str, "%d/%m/%Y").date()
    d_fim = datetime.strptime(dt_fim_str, "%d/%m/%Y").date()
    
    # Identificação B3 vs Internacional
    b3_tickers_set = set()
    if 'Tickers' in empresas_df.columns:
        for row in empresas_df['Tickers'].dropna().astype(str).str.split(','):
            for t in row: b3_tickers_set.add(t.strip().upper())
    
    list_b3 = [t for t in tickers_list if t in b3_tickers_set]
    list_yf = [t for t in tickers_list if t not in b3_tickers_set]
    
    resultados = {}
    erros = []

    # 1. B3 (Engine + Yahoo Adj Close)
    if list_b3:
        # Chama o motor B3 (b3_engine.py)
        dias_uteis = b3_engine.listar_dias_uteis(d_ini, d_fim)
        frames_b3 = []
        with requests.Session() as session:
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = [executor.submit(b3_engine.baixar_e_parsear_dia, d, list_b3, session) for d in dias_uteis]
                for f in futures:
                    res = f.result()
                    if res is not None: frames_b3.append(res)
        
        if frames_b3:
            df_b3_total = pd.concat(frames_b3)
            
            # Yahoo Adj Close
            sa_tickers = [f"{t}.SA" for t in list_b3]
            adj_data = pd.DataFrame()
            try:
                yf_res = yf.download(sa_tickers, start=d_ini, end=d_fim + timedelta(days=5), progress=False)
                if not yf_res.empty:
                    adj_data = yf_res['Adj Close'] if 'Adj Close' in yf_res.columns else yf_res['Close']
            except Exception as e:
                erros.append(f"Aviso Yahoo (Adj): {e}")

            for t in list_b3:
                df_t = df_b3_total[df_b3_total['Ticker'] == t].copy()
                if not df_t.empty:
                    ticker_sa = f"{t}.SA"
                    col_adj = None
                    
                    if not adj_data.empty:
                        if isinstance(adj_data, pd.Series): # Apenas 1 ticker
                            col_adj = adj_data
                        elif ticker_sa in adj_data.columns:
                            col_adj = adj_data[ticker_sa]
                    
                    df_t = df_t.set_index(pd.to_datetime(df_t['Date']))
                    df_t['Adj Close'] = col_adj.reindex(df_t.index) if col_adj is not None else float('nan')
                    df_t = df_t.reset_index()
                    df_t['Date'] = df_t['Date'].dt.strftime('%d/%m/%Y')
                    
                    # Garante ordenação das colunas
                    cols = ['Ticker', 'Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
                    resultados[t] = df_t[[c for c in cols if c in df_t.columns]]
        else:
            erros.append("B3: Sem dados oficiais (feriado ou falha no download do ZIP).")

    # 2. Yahoo (Internacional)
    if list_yf:
        try:
            dados_yf = yf.download(list_yf, start=d_ini, end=d_fim + timedelta(days=1), progress=False)
            if not dados_yf.empty:
                # Tratamento para Yahoo novo (MultiIndex)
                if isinstance(dados_yf.columns, pd.MultiIndex):
                    for t in list_yf:
                        try:
                            df_t = dados_yf.xs(t, axis=1, level=1).copy()
                            df_t['Ticker'] = t
                            df_t = df_t.reset_index()
                            df_t['Date'] = df_t['Date'].dt.strftime('%d/%m/%Y')
                            if 'Adj Close' not in df_t.columns: df_t['Adj Close'] = df_t['Close']
                            resultados[t] = df_t
                        except: pass
                else:
                    t = list_yf[0]
                    dados_yf['Ticker'] = t
                    dados_yf = dados_yf.reset_index()
                    dados_yf['Date'] = dados_yf['Date'].dt.strftime('%d/%m/%Y')
                    resultados[t] = dados_yf
        except Exception as e:
            erros.append(f"Erro Yahoo Intl: {e}")

    return resultados, erros
