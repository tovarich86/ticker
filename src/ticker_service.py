import pandas as pd
import yfinance as yf
import requests
import json
from base64 import b64encode
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from curl_cffi import requests as curl_requests
import streamlit as st 
import time

# Importa o motor de baixo nível
from src import b3_engine

@st.cache_data(ttl=86400)
def carregar_empresas(arquivo_upload=None):
    """
    Carrega a base de empresas da B3 via scraping direto (sem Excel).
    Cache de 24h para evitar sobrecarga na API da B3.
    """
    if arquivo_upload:
        return pd.read_excel(arquivo_upload)

    base_url = "https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetInitialCompanies/"
    all_results = []
    current_page = 1
    total_pages = 1

    session = curl_requests.Session(impersonate="chrome")

    while current_page <= total_pages:
        try:
            params = {"language": "pt-br", "pageNumber": current_page, "pageSize": 100}
            params_encoded = b64encode(json.dumps(params).encode('utf-8')).decode('utf-8')
            response = session.get(f"{base_url}{params_encoded}", timeout=30)
            data = response.json()

            if current_page == 1 and 'page' in data:
                total_pages = data['page'].get('totalPages', 1)

            if 'results' in data:
                all_results.extend(data['results'])

            current_page += 1
        except Exception as e:
            print(f"Erro no scraping B3 (página {current_page}): {e}")
            break

    if not all_results:
        return pd.DataFrame()

    df = pd.DataFrame(all_results)
    df.rename(columns={'tradingName': 'Nome do Pregão', 'issuingCompany': 'CODE'}, inplace=True)
    # Remove espaços e barras para converter "KLABIN S/A" → "KLABINSA" (formato pregão B3)
    df['Nome do Pregão'] = (df['Nome do Pregão'].astype(str).str.strip().str.upper()
                            .str.replace(' ', '', regex=False)
                            .str.replace('/', '', regex=False))
    df['CODE'] = df['CODE'].astype(str).str.strip().str.upper()
    return df[(df['CODE'] != '') & (df['Nome do Pregão'] != '')]

_TIPO_ACAO = {'3': 'ON', '4': 'PN', '5': 'PN', '6': 'PN', '11': 'UNT'}

def get_ticker_info(ticker, df_empresas):
    """Busca informações do ticker separando código base e sufixo."""
    if df_empresas.empty: return None

    ticker_upper = ticker.strip().upper()
    ticker_base = ''.join(c for c in ticker_upper if not c.isdigit())
    ticker_num  = ''.join(c for c in ticker_upper if c.isdigit())

    match = df_empresas[df_empresas['CODE'] == ticker_base]
    if match.empty:
        return None

    row = match.iloc[0]
    return {
        'trading_name': row.get('Nome do Pregão', ''),
        'code': row.get('CODE', ''),
        'type_stock': _TIPO_ACAO.get(ticker_num, ''),
    }


def parece_b3_ticker(ticker: str) -> bool:
    """Retorna True se o ticker tiver padrão B3 por formato (3-4 letras + sufixo válido).
    Não depende de df_empresas — permite encontrar tickers ausentes da API GetInitialCompanies
    (ex: empresas em processo de cancelamento, tickers recentes, SUB3, VLID3, etc.).
    """
    t = ticker.strip().upper()
    base = ''.join(c for c in t if not c.isdigit())
    num  = ''.join(c for c in t if c.isdigit())
    return 3 <= len(base) <= 4 and num in _TIPO_ACAO

def is_b3_ticker(ticker, df_empresas):
    """Retorna True se o ticker for confirmado na base de empresas da B3.
    Usado para proventos/bonificações (requer CODE). Para cotações use parece_b3_ticker().
    """
    ticker_upper = ticker.strip().upper()
    ticker_base = ''.join(c for c in ticker_upper if not c.isdigit())
    ticker_num  = ''.join(c for c in ticker_upper if c.isdigit())
    if ticker_num not in _TIPO_ACAO:
        return False
    return ticker_base in df_empresas['CODE'].values

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

    session = curl_requests.Session(impersonate="chrome120")
    session.headers.update({
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": "https://www.b3.com.br/",
        "Origin": "https://www.b3.com.br",
    })

    while current_page <= total_pages:
        try:
            params = {
                "language": "pt-br",
                "pageNumber": current_page,
                "pageSize": 60,
                "tradingName": trading_name,
            }
            params_json = json.dumps(params, separators=(',', ':'))
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


def buscar_bonificacoes_b3(ticker, empresas_df, data_inicio, data_fim):
    """Busca eventos de bonificação (stock dividends) na B3 de forma robusta."""
    if not any(char.isdigit() for char in ticker):
        return pd.DataFrame()

    ticker_info = get_ticker_info(ticker, empresas_df)
    if not ticker_info or not ticker_info.get('code'):
        st.warning(f"Código (CODE) não encontrado para o ticker {ticker} na planilha. Não é possível buscar bonificações.")
        return pd.DataFrame()

    code = ticker_info['code']
    session = curl_requests.Session(impersonate="chrome120")
    session.headers.update({
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": "https://www.b3.com.br/",
        "Origin": "https://www.b3.com.br",
    })

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

        # Deduplica por data + tipo: a API retorna um registro por ISIN do mesmo ativo
        dedup_cols = [c for c in ['lastDatePrior', 'label'] if c in df.columns]
        if dedup_cols:
            df = df.drop_duplicates(subset=dedup_cols)

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
    tickers_list = [t.strip().upper() for t in tickers_input.split(',') if t.strip()]
    d_ini = datetime.strptime(dt_ini_str, "%d/%m/%Y").date()
    d_fim = datetime.strptime(dt_fim_str, "%d/%m/%Y").date()

    # Roteamento por PADRÃO de ticker (não depende de df_empresas):
    # tickers com formato B3 (3-4 letras + sufixo 3/4/5/6/11) → COTAHIST
    # demais → Yahoo Finance internacional
    # Isso garante que SUB3, VLID3 e outros ausentes da API GetInitialCompanies
    # ainda sejam buscados corretamente via COTAHIST.
    list_b3 = [t for t in tickers_list if parece_b3_ticker(t)]
    list_yf = [t for t in tickers_list if not parece_b3_ticker(t)]
    
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
                    
                    cols = ['Ticker', 'Date', 'Open', 'High', 'Low', 'Close', 'Average', 'Adj Close', 'Volume', 'Quantity']
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

def buscar_dividendos_yf(ticker: str, t0: pd.Timestamp, t1: pd.Timestamp) -> pd.DataFrame:
    """Busca dividendos de ativos internacionais via Yahoo Finance no período [t0, t1]."""
    try:
        divs = yf.Ticker(ticker).dividends
        if divs.empty:
            return pd.DataFrame()
        divs = divs.reset_index()
        divs.columns = ['Date', 'value']
        divs['Date'] = pd.to_datetime(divs['Date']).dt.tz_localize(None)
        divs = divs[(divs['Date'] >= t0) & (divs['Date'] <= t1)].copy()
        
        if divs.empty:
            return pd.DataFrame()
            
        divs['Ticker'] = ticker
        divs['lastDatePriorEx'] = divs['Date'].dt.strftime('%d/%m/%Y')
        divs['paymentDate'] = ''
        divs['label'] = 'Dividendo (YF)'
        divs['typeStock'] = ''
        
        return divs[['Ticker', 'lastDatePriorEx', 'paymentDate', 'label', 'value']]
    except Exception as e:
        return pd.DataFrame()

def buscar_splits_yf(ticker: str, t0: pd.Timestamp, t1: pd.Timestamp) -> pd.DataFrame:
    """Busca eventos de split/reverse split via Yahoo Finance no período [t0, t1]."""
    try:
        splits = yf.Ticker(ticker).splits
        if splits.empty:
            return pd.DataFrame()
        splits = splits.reset_index()
        splits.columns = ['Date', 'factor']
        splits['Date'] = pd.to_datetime(splits['Date']).dt.tz_localize(None)
        splits = splits[(splits['Date'] >= t0) & (splits['Date'] <= t1)].copy()
        
        if splits.empty:
            return pd.DataFrame()
            
        splits['Ticker'] = ticker
        splits['lastDatePrior'] = splits['Date'].dt.strftime('%d/%m/%Y')
        splits['label'] = 'SPLIT (YF)'
        
        # Mantém factor e ratio visíveis
        return splits[['Ticker', 'lastDatePrior', 'label', 'factor']]
    except Exception as e:
        return pd.DataFrame()
