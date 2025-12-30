import streamlit as st
import pandas as pd
import yfinance as yf
import b3_engine  # Certifique-se que b3_engine.py está na mesma pasta
import requests
import json
import time
from base64 import b64encode
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from curl_cffi import requests as curl_requests
from io import BytesIO

# URL do arquivo no GitHub com a lista de empresas
URL_EMPRESAS = "https://github.com/tovarich86/ticker/raw/refs/heads/main/empresas_b3%20(6).xlsx"

@st.cache_data
def carregar_empresas():
    try:
        df_empresas = pd.read_excel(URL_EMPRESAS)
        cols_to_process = ['Nome do Pregão', 'Tickers', 'CODE', 'typeStock']
        for col in cols_to_process:
            if col in df_empresas.columns:
                df_empresas[col] = df_empresas[col].astype(str).fillna('').str.strip()
                if col == 'Nome do Pregão':
                    df_empresas[col] = df_empresas[col].str.replace(r'\s*S\.?A\.?/A?', ' S.A.', regex=True).str.upper()
                if col == 'typeStock':
                    df_empresas[col] = df_empresas[col].str.upper()
        return df_empresas[(df_empresas['Tickers'] != '') & (df_empresas['Nome do Pregão'] != '')]
    except Exception as e:
        st.error(f"Erro ao carregar empresas: {e}")
        return pd.DataFrame()

def get_ticker_info(ticker, empresas_df):
    ticker_upper = ticker.strip().upper()
    for _, row in empresas_df.iterrows():
        tickers_list = [t.strip().upper() for t in row['Tickers'].split(",") if t.strip()]
        if ticker_upper in tickers_list:
            return {'trading_name': row['Nome do Pregão'], 'code': row['CODE'], 'type_stock': row['typeStock']}
    return None

# --- FUNÇÕES DE DIVIDENDOS E BONIFICAÇÕES (B3) ---
def buscar_dividendos_b3(ticker, empresas_df, data_inicio, data_fim):
    ticker_info = get_ticker_info(ticker, empresas_df)
    if not ticker_info: return pd.DataFrame()
    
    trading_name = ticker_info['trading_name']
    desired_type_stock = ticker_info['type_stock']
    
    session = curl_requests.Session(impersonate="chrome")
    try:
        params = {"language": "pt-br", "pageNumber": "1", "pageSize": "50", "tradingName": trading_name}
        params_encoded = b64encode(json.dumps(params).encode('utf-8')).decode('utf-8')
        url = f'https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetListedCashDividends/{params_encoded}'
        response = session.get(url, timeout=30)
        res_json = response.json()
        if 'results' in res_json:
            df = pd.DataFrame(res_json['results'])
            if df.empty: return pd.DataFrame()
            df['typeStock'] = df['typeStock'].str.strip().str.upper()
            df = df[df['typeStock'] == desired_type_stock].copy()
            df['lastDatePriorEx_dt'] = pd.to_datetime(df['lastDatePriorEx'], format='%d/%m/%Y', errors='coerce')
            df = df[(df['lastDatePriorEx_dt'] >= data_inicio) & (df['lastDatePriorEx_dt'] <= data_fim)]
            return df.drop(columns=['lastDatePriorEx_dt'])
    except: return pd.DataFrame()
    return pd.DataFrame()

def buscar_bonificacoes_b3(ticker, empresas_df, data_inicio, data_fim):
    ticker_info = get_ticker_info(ticker, empresas_df)
    if not ticker_info or not ticker_info.get('code'): return pd.DataFrame()
    
    session = curl_requests.Session(impersonate="chrome")
    try:
        params = {"issuingCompany": ticker_info['code'], "language": "pt-br"}
        params_encoded = b64encode(json.dumps(params).encode('utf-8')).decode('utf-8')
        url = f'https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetListedSupplementCompany/{params_encoded}'
        response = session.get(url, timeout=30)
        data = response.json()
        if data and "stockDividends" in data[0]:
            df = pd.DataFrame(data[0]["stockDividends"])
            if df.empty: return pd.DataFrame()
            df['lastDatePrior_dt'] = pd.to_datetime(df['lastDatePrior'], format='%d/%m/%Y', errors='coerce')
            df = df[(df['lastDatePrior_dt'] >= data_inicio) & (df['lastDatePrior_dt'] <= data_fim)]
            return df.drop(columns=['lastDatePrior_dt'])
    except: return pd.DataFrame()
    return pd.DataFrame()

# --- LÓGICA HÍBRIDA COM TRATAMENTO DE ERRO PARA ADJ CLOSE ---
def buscar_dados_hibrido(tickers_input, data_inicio_input, data_fim_input, empresas_df):
    tickers_list = [t.strip().upper() for t in tickers_input.split(',') if t.strip()]
    d_ini = datetime.strptime(data_inicio_input, "%d/%m/%Y").date()
    d_fim = datetime.strptime(data_fim_input, "%d/%m/%Y").date()
    
    b3_tickers_set = set()
    for row in empresas_df['Tickers'].dropna().str.split(','):
        for t in row: b3_tickers_set.add(t.strip().upper())
    
    list_b3 = [t for t in tickers_list if t in b3_tickers_set]
    list_yf = [t for t in tickers_list if t not in b3_tickers_set]
    
    resultados = {}
    erros = []

    # 1. Processamento B3 (OHLCV da B3 + Adj Close do Yahoo Seguro)
    if list_b3:
        st.info(f"Extraindo dados brutos da B3 para {len(list_b3)} ativos...")
        dias_uteis = b3_engine.listar_dias_uteis(d_ini, d_fim)
        frames_b3 = []
        with requests.Session() as session:
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = [executor.submit(b3_engine.baixar_e_parsear_dia, d, list_b3, session) for d in dias_uteis]
                for f in futures:
                    res = f.result()
                    if res is not None: frames_b3.append(res)
        
        if frames_b3:
            df_b3_total = pd.concat(frames_b3)
            
            # Busca Fechamento Ajustado no Yahoo de forma segura
            sa_tickers = [f"{t}.SA" for t in list_b3]
            adj_data = pd.DataFrame()
            try:
                yf_res = yf.download(sa_tickers, start=d_ini, end=d_fim + timedelta(days=1), progress=False)
                if not yf_res.empty and 'Adj Close' in yf_res.columns:
                    adj_data = yf_res['Adj Close']
            except Exception as e:
                st.warning(f"Aviso: Não foi possível obter o 'Adj Close' do Yahoo Finance ({e}).")

            for t in list_b3:
                df_t = df_b3_total[df_b3_total['Ticker'] == t].copy()
                if not df_t.empty:
                    ticker_sa = f"{t}.SA"
                    col_adj = None
                    
                    if not adj_data.empty:
                        if isinstance(adj_data, pd.Series): # Apenas 1 ticker
                            col_adj = adj_data
                        elif ticker_sa in adj_data.columns: # Múltiplos tickers
                            col_adj = adj_data[ticker_sa]
                    
                    df_t = df_t.set_index(pd.to_datetime(df_t['Date']))
                    df_t['Adj Close'] = col_adj if col_adj is not None else float('nan')
                    df_t = df_t.reset_index(drop=True)
                    df_t['Date'] = pd.to_datetime(df_t['Date']).dt.strftime('%d/%m/%Y')
                    
                    cols = ['Ticker', 'Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
                    resultados[t] = df_t[[c for c in cols if c in df_t.columns]]
        else: erros.append("Sem dados brutos na B3 para o período informado.")

    # 2. Processamento Yahoo (Internacional)
    if list_yf:
        st.info(f"Buscando {len(list_yf)} ativos internacionais no Yahoo Finance...")
        try:
            dados_yf = yf.download(list_yf, start=d_ini, end=d_fim + timedelta(days=1), progress=False)
            if not dados_yf.empty:
                for t in list_yf:
                    df_t = dados_yf.loc[:, (slice(None), t)].copy() if len(list_yf) > 1 else dados_yf.copy()
                    if len(list_yf) > 1: df_t.columns = df_t.columns.droplevel(1)
                    df_t = df_t.reset_index()
                    df_t['Ticker'], df_t['Date'] = t, df_t['Date'].dt.strftime('%d/%m/%Y')
                    # Garante a existência da coluna Adj Close
                    if 'Adj Close' not in df_t.columns: df_t['Adj Close'] = df_t['Close']
                    resultados[t] = df_t[['Ticker', 'Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']]
        except Exception as e: erros.append(f"Erro Yahoo: {e}")

    return resultados, erros

# --- INTERFACE STREAMLIT ---
st.set_page_config(layout="wide", page_title="Market Data B3/YF")
st.title('Consulta Consolidada de Dados')

st.warning("""
**Fontes de Dados:**
* **Ativos Brasileiros (B3):** OHLCV extraídos do arquivo oficial **COTAHIST/B3**.
* **Ativos Internacionais:** Todos os dados via **Yahoo Finance**.
* **Adj Close:** Obtido via **Yahoo Finance** (pode estar ausente se não houver dados no Yahoo).
* **Proventos:** Extraídos via API de Empresas Listadas da **B3**.
""")

df_empresas = carregar_empresas()

col1, col2 = st.columns(2)
with col1: tickers_input = st.text_input("Tickers (PETR4, AAPL, ITUB4):", placeholder="Separe por vírgula")
with col2: tipos = st.multiselect("Dados:", ["Preços", "Dividendos", "Bonificações"], default=["Preços"])

col3, col4 = st.columns(2)
with col3: data_ini_str = st.text_input("Início (dd/mm/aaaa):", value=(datetime.now() - timedelta(days=10)).strftime("%d/%m/%Y"))
with col4: data_fim_str = st.text_input("Fim (dd/mm/aaaa):", value=datetime.now().strftime("%d/%m/%Y"))

if st.button('Executar Busca'):
    if tickers_input and data_ini_str and data_fim_str:
        try:
            d_ini_dt = datetime.strptime(data_ini_str, "%d/%m/%Y")
            d_fim_dt = datetime.strptime(data_fim_str, "%d/%m/%Y")
        except ValueError:
            st.error("Formato de data inválido. Use dd/mm/aaaa.")
            st.stop()

        st.session_state.todos_dados_acoes = {}
        st.session_state.todos_dados_dividendos = {}
        st.session_state.todos_dados_bonificacoes = {}
        
        with st.spinner('Processando...'):
            if "Preços" in tipos:
                res, err = buscar_dados_hibrido(tickers_input, data_ini_str, data_fim_str, df_empresas)
                st.session_state.todos_dados_acoes = res
                for e in err: st.error(e)
            
            tickers_list = [t.strip().upper() for t in tickers_input.split(',') if t.strip()]
            for t in tickers_list:
                if "Dividendos" in tipos:
                    df_div = buscar_dividendos_b3(t, df_empresas, d_ini_dt, d_fim_dt)
                    if not df_div.empty: st.session_state.todos_dados_dividendos[t] = df_div
                if "Bonificações" in tipos:
                    df_bon = buscar_bonificacoes_b3(t, df_empresas, d_ini_dt, d_fim_dt)
                    if not df_bon.empty: st.session_state.todos_dados_bonificacoes[t] = df_bon
        st.success("Busca finalizada!")

# --- EXIBIÇÃO E DOWNLOAD ---
if st.session_state.get('todos_dados_acoes'):
    st.subheader("Cotações")
    st.dataframe(pd.concat(st.session_state.todos_dados_acoes.values(), ignore_index=True))

if st.session_state.get('todos_dados_dividendos'):
    st.subheader("Dividendos (B3)")
    st.dataframe(pd.concat(st.session_state.todos_dados_dividendos.values(), ignore_index=True))

if st.session_state.get('todos_dados_bonificacoes'):
    st.subheader("Bonificações (B3)")
    st.dataframe(pd.concat(st.session_state.todos_dados_bonificacoes.values(), ignore_index=True))

if any([st.session_state.get('todos_dados_acoes'), st.session_state.get('todos_dados_dividendos')]):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        if st.session_state.todos_dados_acoes:
            pd.concat(st.session_state.todos_dados_acoes.values(), ignore_index=True).to_excel(writer, sheet_name="Precos", index=False)
        if st.session_state.todos_dados_dividendos:
            pd.concat(st.session_state.todos_dados_dividendos.values(), ignore_index=True).to_excel(writer, sheet_name="Dividendos", index=False)
        if st.session_state.todos_dados_bonificacoes:
            pd.concat(st.session_state.todos_dados_bonificacoes.values(), ignore_index=True).to_excel(writer, sheet_name="Bonificacoes", index=False)
    
    st.download_button("Baixar Relatório Excel", data=output.getvalue(), file_name="relatorio_mercado.xlsx")
