import streamlit as st
import pandas as pd
import yfinance as yf
import b3_engine  # Nosso motor nativo
import requests
import json
import time
from base64 import b64encode
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from curl_cffi import requests as curl_requests

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

# --- NOVA LOGICA HIBRIDA DE PREÇOS ---
def buscar_dados_hibrido(tickers_input, data_inicio_input, data_fim_input, empresas_df):
    tickers_list = [t.strip().upper() for t in tickers_input.split(',') if t.strip()]
    d_ini = datetime.strptime(data_inicio_input, "%d/%m/%Y").date()
    d_fim = datetime.strptime(data_fim_input, "%d/%m/%Y").date()
    
    # Identifica quais tickers pertencem à B3
    b3_tickers_set = set()
    for row in empresas_df['Tickers'].dropna().str.split(','):
        for t in row: b3_tickers_set.add(t.strip().upper())
    
    list_b3 = [t for t in tickers_list if t in b3_tickers_set]
    list_yf = [t for t in tickers_list if t not in b3_tickers_set]
    
    resultados = {}
    erros = []

    # 1. Busca Nativa B3 (COTAHIST)
    if list_b3:
        st.info(f"Buscando {len(list_b3)} ativos diretamente nos servidores da B3...")
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
            df_b3_total['Date'] = pd.to_datetime(df_b3_total['Date']).dt.strftime('%d/%m/%Y')
            for t in list_b3:
                df_t = df_b3_total[df_b3_total['Ticker'] == t].copy()
                if not df_t.empty: resultados[t] = df_t
        else: erros.append("Nenhum dado de preço encontrado na B3 para o período.")

    # 2. Busca Yahoo Finance (Internacionais)
    if list_yf:
        st.info(f"Buscando {len(list_yf)} ativos no Yahoo Finance...")
        try:
            dados_yf = yf.download(list_yf, start=d_ini, end=d_fim + timedelta(days=1), progress=False)
            if not dados_yf.empty:
                for t in list_yf:
                    df_t = dados_yf.loc[:, (slice(None), t)].copy() if len(list_yf) > 1 else dados_yf.copy()
                    if len(list_yf) > 1: df_t.columns = df_t.columns.droplevel(1)
                    df_t = df_t.reset_index()
                    df_t['Ticker'], df_t['Date'] = t, df_t['Date'].dt.strftime('%d/%m/%Y')
                    resultados[t] = df_t[['Ticker', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
        except Exception as e: erros.append(f"Erro Yahoo Finance: {e}")

    return resultados, erros

# --- FUNÇÕES DE DIVIDENDOS E BONIFICAÇÕES (MANTIDAS) ---
def buscar_dividendos_b3(ticker, empresas_df, data_inicio, data_fim):
    # (Mantenha aqui a sua função buscar_dividendos_b3 original na íntegra)
    pass # ... (código original omitido para brevidade, mas deve ser incluído)

def buscar_bonificacoes_b3(ticker, empresas_df, data_inicio, data_fim):
    # (Mantenha aqui a sua função buscar_bonificacoes_b3 original na íntegra)
    pass # ... (código original omitido para brevidade, mas deve ser incluído)

# --- INTERFACE STREAMLIT ---
st.set_page_config(layout="wide")
st.title('Consulta Consolidada: B3 Direta & Yahoo Finance')

df_empresas = carregar_empresas()

col1, col2 = st.columns(2)
with col1: tickers_input = st.text_input("Tickers (ex: PETR4, AAPL, ITUB4):")
with col2: tipos_dados = st.multiselect("Dados:", ["Preços Históricos", "Dividendos (B3)", "Bonificações (B3)"], default=["Preços Históricos"])

col3, col4 = st.columns(2)
with col3: data_ini = st.text_input("Data Início (dd/mm/aaaa):")
with col4: data_fim = st.text_input("Data Fim (dd/mm/aaaa):")

if st.button('Buscar Dados'):
    if tickers_input and data_ini and data_fim:
        st.session_state.todos_dados_acoes = {}
        st.session_state.todos_dados_dividendos = {}
        st.session_state.todos_dados_bonificacoes = {}
        
        with st.spinner('Processando requisições...'):
            # Preços
            if "Preços Históricos" in tipos_dados:
                res, err = buscar_dados_hibrido(tickers_input, data_ini, data_fim, df_empresas)
                st.session_state.todos_dados_acoes = res
                for e in err: st.error(e)
            
            # Dividendos e Bonificações
            tickers_list = [t.strip().upper() for t in tickers_input.split(',') if t.strip()]
            d_ini_dt = datetime.strptime(data_ini, "%d/%m/%Y")
            d_fim_dt = datetime.strptime(data_fim, "%d/%m/%Y")
            
            for t in tickers_list:
                if "Dividendos (B3)" in tipos_dados:
                    df_div = buscar_dividendos_b3(t, df_empresas, d_ini_dt, d_fim_dt)
                    if not df_div.empty: st.session_state.todos_dados_dividendos[t] = df_div
                if "Bonificações (B3)" in tipos_dados:
                    df_bon = buscar_bonificacoes_b3(t, df_empresas, d_ini_dt, d_fim_dt)
                    if not df_bon.empty: st.session_state.todos_dados_bonificacoes[t] = df_bon
        
        st.success("Busca concluída!")
    else:
        st.warning("Preencha todos os campos obrigatórios.")

# --- EXIBIÇÃO E DOWNLOAD (MANTIDO CONFORME ORIGINAL) ---
# ... (Insira aqui a lógica de exibição de DataFrames e geração de Excel do seu código original)
