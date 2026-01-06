import pandas as pd
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
URL_EMPRESAS = "https://github.com/tovarich86/ticker/raw/refs/heads/main/assets/empresas_b3%20(6).xlsx"

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
    """
    Busca dividendos na B3 para um ticker específico, tratando paginação
    e filtrando pelo typeStock correto (ON, PN, UNT).
    Retorna um DataFrame com os dividendos filtrados ou DataFrame vazio.
    """
    if not any(char.isdigit() for char in ticker):
        return pd.DataFrame()

    ticker_info = get_ticker_info(ticker, empresas_df)

    if not ticker_info:
        st.warning(f"Informações não encontradas para o ticker {ticker} na planilha de empresas.")
        return pd.DataFrame()

    trading_name = ticker_info['trading_name']
    desired_type_stock = ticker_info['type_stock']

    if not trading_name:
        st.warning(f"Nome de pregão não encontrado para o ticker {ticker}.")
        return pd.DataFrame()
    if not desired_type_stock:
        st.warning(f"Tipo de ação (typeStock) não encontrado para o ticker {ticker} na planilha.")
        return pd.DataFrame()

    all_dividends = []
    current_page = 1
    total_pages = 1

    st.write(f"Buscando dividendos para {ticker} ({trading_name}, Tipo: {desired_type_stock})...")

    # Usa curl_cffi para simular um navegador na chamada à B3
    session = curl_requests.Session(impersonate="chrome")
    
    while current_page <= total_pages:
        try:
            params = {
                "language": "pt-br",
                "pageNumber": str(current_page),
                "pageSize": "50",
                "tradingName": trading_name,
            }
            params_json = json.dumps(params)
            params_encoded = b64encode(params_json.encode('utf-8')).decode('utf-8')
            url = f'https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetListedCashDividends/{params_encoded}'

            response = session.get(url, timeout=30)
            response.raise_for_status()
            response_json = response.json()

            if current_page == 1 and 'page' in response_json and 'totalPages' in response_json['page']:
                total_pages = int(response_json['page']['totalPages'])
                st.write(f"Total de {total_pages} páginas de dividendos encontradas para {trading_name}.")

            if 'results' in response_json and response_json['results']:
                all_dividends.extend(response_json['results'])
            elif current_page == 1:
                break

            if total_pages > 1:
                time.sleep(0.2)

            current_page += 1

        except curl_requests.errors.RequestsError as e:
            st.error(f"Erro de rede ao buscar dividendos para {ticker} (página {current_page}): {e}")
            break
        except json.JSONDecodeError:
            st.error(f"Erro ao decodificar JSON da resposta da B3 para {ticker} (página {current_page}).")
            break
        except Exception as e:
            st.error(f"Erro inesperado ao buscar dividendos para {ticker} (página {current_page}): {e}")
            break

    if not all_dividends:
        return pd.DataFrame()

    df = pd.DataFrame(all_dividends)
    if 'typeStock' in df.columns:
        df['typeStock'] = df['typeStock'].str.strip().str.upper()
        df_filtered_type = df[df['typeStock'] == desired_type_stock].copy()
        if df_filtered_type.empty:
            return pd.DataFrame()
        df = df_filtered_type
    else:
        st.warning(f"Coluna 'typeStock' não encontrada nos resultados da B3 para {ticker}. Não foi possível filtrar por tipo de ação.")

    df['Ticker'] = ticker

    if 'lastDatePriorEx' in df.columns:
        df['lastDatePriorEx_dt'] = pd.to_datetime(df['lastDatePriorEx'], format='%d/%m/%Y', errors='coerce')
        df = df.dropna(subset=['lastDatePriorEx_dt'])
        df = df[(df['lastDatePriorEx_dt'] >= data_inicio) & (df['lastDatePriorEx_dt'] <= data_fim)]
        df = df.drop(columns=['lastDatePriorEx_dt'])
    else:
        st.warning(f"Coluna 'lastDatePriorEx' não encontrada para filtrar datas de dividendos de {ticker}.")
        return pd.DataFrame()

    if 'Ticker' in df.columns:
        cols_to_keep = ['Ticker', 'paymentDate', 'typeStock', 'lastDatePriorEx', 'value', 'relatedToAction', 'label', 'ratio']
        existing_cols_to_keep = [col for col in cols_to_keep if col in df.columns]
        other_cols = [col for col in df.columns if col not in existing_cols_to_keep]
        df = df[existing_cols_to_keep + other_cols]

    return df

Para tornar a busca de bonificações robusta como no seu código original e resolver definitivamente o erro de duplicidade da coluna "Ticker", você deve atualizar o arquivo de serviço e ajustar a interface.

Aqui estão as correções detalhadas:

1. Atualização do src/ticker_service.py
Esta versão de buscar_bonificacoes_b3 reintegra o tratamento de erros detalhado, a filtragem rigorosa e a organização de colunas do seu código original, garantindo que o DataFrame já saia da função com o campo 'Ticker'.

Python

def buscar_bonificacoes_b3(ticker, empresas_df, data_inicio, data_fim):
    """Busca eventos de bonificação (stock dividends) na B3 de forma robusta."""
    if not any(char.isdigit() for char in ticker):
        return pd.DataFrame()

    ticker_info = get_ticker_info(ticker, empresas_df)
    if not ticker_info or not ticker_info.get('code'):
        st.warning(f"Código (CODE) não encontrado para o ticker {ticker} na planilha. Não é possível buscar bonificações.")
        return pd.DataFrame()

    code = ticker_info['code']
    session = curl_requests.Session(impersonate="chrome")

    try:
        params_bonificacoes = {
            "issuingCompany": code,
            "language": "pt-br"
        }
        params_json = json.dumps(params_bonificacoes)
        params_encoded = b64encode(params_json.encode('utf-8')).decode('utf-8')
        url = f'https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetListedSupplementCompany/{params_encoded}'
        
        response = session.get(url, timeout=30)
        response.raise_for_status()

        if not response.content or not response.text.strip():
            return pd.DataFrame()
        
        try:
            data = response.json()
        except json.JSONDecodeError:
            return pd.DataFrame()

        if not isinstance(data, list) or not data or "stockDividends" not in data[0] or not data[0]["stockDividends"]:
            return pd.DataFrame()

        df = pd.DataFrame(data[0]["stockDividends"])
        if df.empty:
            return pd.DataFrame()

        # Adiciona o Ticker internamente
        df['Ticker'] = ticker
        
        if 'lastDatePrior' in df.columns:
            df['lastDatePrior_dt'] = pd.to_datetime(df['lastDatePrior'], format='%d/%m/%Y', errors='coerce')
            df = df.dropna(subset=['lastDatePrior_dt'])
            # Filtro de data
            df = df[(df['lastDatePrior_dt'] >= pd.to_datetime(data_inicio)) & (df['lastDatePrior_dt'] <= pd.to_datetime(data_fim))]
            df = df.drop(columns=['lastDatePrior_dt'])
        else:
            st.warning(f"Coluna 'lastDatePrior' não encontrada para filtrar datas de bonificações de {ticker}.")
            return pd.DataFrame()

        # Reordenação de colunas para padrão profissional
        cols_to_keep = ['Ticker', 'label', 'lastDatePrior', 'factor', 'approvedIn', 'isinCode']
        existing_cols_to_keep = [col for col in cols_to_keep if col in df.columns]
        other_cols = [col for col in df.columns if col not in existing_cols_to_keep]
        df = df[existing_cols_to_keep + other_cols]

        return df

    except curl_requests.errors.RequestsError as e:
        st.error(f"Erro de rede ao buscar bonificações para {ticker} (Código: {code}): {e}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Erro inesperado ao buscar bonificações para {ticker} (Código: {code}): {e}")
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
                    # --- CORREÇÃO DO ERRO RESET_INDEX ---
                    # 1. Converte para datetime
                    df_t['Date'] = pd.to_datetime(df_t['Date'])
                    # 2. Move para o índice (remove da coluna, evitando duplicação)
                    df_t = df_t.set_index('Date')
                    
                    ticker_sa = f"{t}.SA"
                    col_adj = None
                    
                    if not adj_data.empty:
                        if isinstance(adj_data, pd.Series): 
                            col_adj = adj_data
                        elif ticker_sa in adj_data.columns:
                            col_adj = adj_data[ticker_sa]
                    
                    # 3. Reindexa e atribui Adj Close
                    df_t['Adj Close'] = col_adj.reindex(df_t.index) if col_adj is not None else float('nan')
                    
                    # 4. Reseta índice (traz Date de volta como coluna sem conflito)
                    df_t = df_t.reset_index()
                    df_t['Date'] = df_t['Date'].dt.strftime('%d/%m/%Y')
                    
                    cols = ['Ticker', 'Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
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

    return resultados, erros
