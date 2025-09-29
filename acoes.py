import streamlit as st
import pandas as pd
import yfinance as yf
from base64 import b64encode
from datetime import datetime, timedelta
import json
import time

# Importa curl_cffi para criar sessão com fingerprint de navegador para as APIs da B3
from curl_cffi import requests as curl_requests

# URL do arquivo no GitHub com a lista de empresas
URL_EMPRESAS = "https://github.com/tovarich86/ticker/raw/refs/heads/main/empresas_b3%20(6).xlsx"

@st.cache_data
def carregar_empresas():
    """Carrega e pré-processa o DataFrame de empresas a partir de um arquivo Excel."""
    try:
        df_empresas = pd.read_excel(URL_EMPRESAS)

        # Padronizar colunas de texto e remover espaços extras
        cols_to_process = ['Nome do Pregão', 'Tickers', 'CODE', 'typeStock']
        for col in cols_to_process:
            if col in df_empresas.columns:
                # Garantir que a coluna seja string e preencher NaNs com string vazia
                df_empresas[col] = df_empresas[col].astype(str).fillna('')
                # Remover espaços extras no início/fim
                df_empresas[col] = df_empresas[col].str.strip()
                # Padronizar Nome do Pregão para S.A. e maiúsculas
                if col == 'Nome do Pregão':
                    df_empresas[col] = df_empresas[col].str.replace(r'\s*S\.?A\.?/A?', ' S.A.', regex=True).str.upper().str.strip()
                # Padronizar typeStock para maiúsculas
                if col == 'typeStock':
                    df_empresas[col] = df_empresas[col].str.upper()

        # Remover linhas onde Tickers ou Nome do Pregão estão vazios após limpeza
        df_empresas = df_empresas[df_empresas['Tickers'] != '']
        df_empresas = df_empresas[df_empresas['Nome do Pregão'] != '']

        return df_empresas
    except Exception as e:
        st.error(f"Erro ao carregar ou processar a planilha de empresas: {e}")
        return pd.DataFrame() # Retorna DataFrame vazio em caso de erro

def get_ticker_info(ticker, empresas_df):
    """
    Busca informações de um ticker (Nome do Pregão, CODE, typeStock) na planilha de empresas.
    Retorna um dicionário com as informações ou None se não encontrado.
    """
    ticker_upper = ticker.strip().upper()
    for index, row in empresas_df.iterrows():
        # Divide a string de tickers, remove espaços e converte para maiúsculas
        tickers_list = [t.strip().upper() for t in row['Tickers'].split(",") if t.strip()]
        if ticker_upper in tickers_list:
            return {
                'trading_name': row['Nome do Pregão'],
                'code': row['CODE'],
                'type_stock': row['typeStock']
            }
    return None  # Retorna None se o ticker não for encontrado

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
                time.sleep(0.5)

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
    """Busca eventos de bonificação (stock dividends) na B3 usando o CODE da empresa."""
    if not any(char.isdigit() for char in ticker):
        return pd.DataFrame()

    ticker_info = get_ticker_info(ticker, empresas_df)
    if not ticker_info or not ticker_info.get('code'):
        st.warning(f"Código (CODE) não encontrado para o ticker {ticker} na planilha. Não é possível buscar bonificações.")
        return pd.DataFrame()

    code = ticker_info['code']

    # Usa curl_cffi para simular um navegador na chamada à B3
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

        df['Ticker'] = ticker
        if 'lastDatePrior' in df.columns:
            df['lastDatePrior_dt'] = pd.to_datetime(df['lastDatePrior'], format='%d/%m/%Y', errors='coerce')
            df = df.dropna(subset=['lastDatePrior_dt'])
            df = df[(df['lastDatePrior_dt'] >= data_inicio) & (df['lastDatePrior_dt'] <= data_fim)]
            df = df.drop(columns=['lastDatePrior_dt'])
        else:
            st.warning(f"Coluna 'lastDatePrior' não encontrada para filtrar datas de bonificações de {ticker}.")
            return pd.DataFrame()

        if 'Ticker' in df.columns:
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

# --- Função de Busca de Dados de Ações (CORRIGIDA) ---
def buscar_dados_acoes(tickers_input, data_inicio_input, data_fim_input, empresas_df, st=None):
    """
    Busca o histórico de preços de ações usando o yfinance, permitindo que a biblioteca
    gerencie sua própria sessão de rede.
    """
    try:
        # Converte as datas para o formato que o yfinance espera (YYYY-MM-DD)
        data_inicio_str = datetime.strptime(data_inicio_input, "%d/%m/%Y").strftime("%Y-%m-%d")
        data_fim_dt = datetime.strptime(data_fim_input, "%d/%m/%Y")
        # Adiciona 1 dia à data final para garantir que o último dia seja incluído no resultado
        data_fim_ajustada_str = (data_fim_dt + timedelta(days=1)).strftime("%Y-%m-%d")
    except ValueError:
        if st:
            st.error("Formato de data inválido. Use dd/mm/aaaa.")
        return {}, ["Formato de data inválido."]

    # Limpa e formata a lista de tickers
    tickers_list = [ticker.strip().upper() for ticker in tickers_input.split(',') if ticker.strip()]
    dados_acoes_dict = {}
    erros = []

    # Cria um conjunto de tickers da B3 para adicionar o sufixo '.SA'
    b3_tickers_set = set()
    if 'Tickers' in empresas_df.columns:
        for t_list in empresas_df['Tickers'].dropna().str.split(','):
            for ticker in t_list:
                if ticker.strip():
                    b3_tickers_set.add(ticker.strip().upper())
    
    # Adiciona o sufixo '.SA' para tickers brasileiros reconhecidos
    tickers_yf = [f"{ticker}.SA" if ticker in b3_tickers_set else ticker for ticker in tickers_list]

    try:
        if st:
            st.write(f"Buscando preços históricos para {', '.join(tickers_list)}...")
        else:
            print(f"Buscando preços históricos para {', '.join(tickers_list)}...")
        
        # A chamada ao yf.download agora é feita sem sessão customizada.
        # Isso permite que o yfinance use sua própria lógica de conexão, que é mais robusta.
        dados = yf.download(
            tickers=tickers_yf,
            start=data_inicio_str,
            end=data_fim_ajustada_str,
            auto_adjust=False, # Manter False para obter 'Adj Close' e 'Close'
            progress=False
        )
        
    except Exception as e:
        error_type = type(e).__name__
        return {}, [f"Erro ao baixar dados de preços do Yahoo Finance: {error_type} - {e}"]

    # Se o download falhar para todos os tickers, 'dados' pode estar vazio
    if dados.empty:
        erros.append("Nenhum dado de preço foi retornado pelo Yahoo Finance para os tickers solicitados.")
        return {}, erros

    # Processa os dados para cada ticker individualmente
    for idx, ticker in enumerate(tickers_list):
        ticker_yf = tickers_yf[idx]
        try:
            # Se mais de um ticker foi baixado, os dados têm um MultiIndex nas colunas
            if isinstance(dados.columns, pd.MultiIndex):
                dados_ticker = dados.loc[:, (slice(None), ticker_yf)]
                dados_ticker.columns = dados_ticker.columns.droplevel(1)
            else: # Se apenas um ticker foi baixado
                dados_ticker = dados.copy()

            dados_ticker = dados_ticker.dropna(how='all')

            if not dados_ticker.empty:
                dados_ticker = dados_ticker.reset_index()
                dados_ticker = dados_ticker[dados_ticker['Date'] <= data_fim_dt]
                
                dados_ticker['Date'] = pd.to_datetime(dados_ticker['Date']).dt.strftime('%d/%m/%Y')
                dados_ticker['Ticker'] = ticker

                # Reordena as colunas para um formato mais legível
                standard_cols = ['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
                cols_order_start = ['Ticker', 'Date']
                existing_standard_cols = [col for col in standard_cols if col in dados_ticker.columns]
                other_cols = [col for col in dados_ticker.columns if col not in cols_order_start and col not in existing_standard_cols]
                final_cols_order = cols_order_start + existing_standard_cols + other_cols
                dados_ticker = dados_ticker[final_cols_order]

                dados_acoes_dict[ticker] = dados_ticker
            else:
                erros.append(f"Sem dados de preços históricos encontrados para {ticker} ({ticker_yf}) no período.")
        except KeyError:
            erros.append(f"Dados para {ticker} ({ticker_yf}) não encontrados no resultado. O ticker pode ser inválido.")
        except Exception as e:
            error_type = type(e).__name__
            erros.append(f"Erro ao processar dados de preços para {ticker} ({ticker_yf}): {error_type} - {e}")

    return dados_acoes_dict, erros

# ============================================
# Interface do Streamlit
# ============================================
st.set_page_config(layout="wide")
st.title('Consulta Dados de Mercado B3 e Preço Yahoo Finance')

# --- Carrega o DataFrame de empresas ---
df_empresas = carregar_empresas()

if df_empresas.empty:
    st.error("Não foi possível carregar a lista de empresas. Verifique a URL ou o arquivo. A aplicação não pode continuar.")
    st.stop()

# --- Entradas do Usuário ---
col1, col2 = st.columns(2)
with col1:
    tickers_input = st.text_input("Digite os tickers separados por vírgula (ex: PETR4, VALE3, MGLU3, ITUB4):", key="tickers")
with col2:
    tipos_dados_selecionados = st.multiselect(
        "Selecione os dados que deseja buscar:",
        ["Preços Históricos (Yahoo Finance)", "Dividendos (B3)", "Bonificações (B3)"],
        default=["Preços Históricos (Yahoo Finance)"],
        key="data_types"
    )

col3, col4 = st.columns(2)
with col3:
    data_inicio_input = st.text_input("Data de início (dd/mm/aaaa):", key="date_start")
with col4:
    data_fim_input = st.text_input("Data de fim (dd/mm/aaaa):", key="date_end")

# --- Inicialização do Session State ---
if 'dados_buscados' not in st.session_state:
    st.session_state.dados_buscados = False
    st.session_state.todos_dados_acoes = {}
    st.session_state.todos_dados_dividendos = {}
    st.session_state.todos_dados_bonificacoes = {}
    st.session_state.erros_gerais = []

# --- Botão e Lógica Principal ---
if st.button('Buscar Dados', key="search_button"):
    st.session_state.dados_buscados = False
    st.session_state.todos_dados_acoes = {}
    st.session_state.todos_dados_dividendos = {}
    st.session_state.todos_dados_bonificacoes = {}
    st.session_state.erros_gerais = []

    if tickers_input and data_inicio_input and data_fim_input and tipos_dados_selecionados:
        try:
            data_inicio_dt = datetime.strptime(data_inicio_input, "%d/%m/%Y")
            data_fim_dt = datetime.strptime(data_fim_input, "%d/%m/%Y")
            if data_inicio_dt > data_fim_dt:
                st.error("A data de início não pode ser posterior à data de fim.")
                st.stop()
        except ValueError:
            st.error("Formato de data inválido. Use dd/mm/aaaa.")
            st.stop()

        tickers_list = sorted(list(set([ticker.strip().upper() for ticker in tickers_input.split(',') if ticker.strip()])))

        with st.spinner('Buscando dados... Por favor, aguarde.'):
            if "Preços Históricos (Yahoo Finance)" in tipos_dados_selecionados:
                dados_acoes_dict, erros_acoes = buscar_dados_acoes(tickers_input, data_inicio_input, data_fim_input, empresas_df=df_empresas, st=st)
                if dados_acoes_dict:
                    st.session_state.todos_dados_acoes = dados_acoes_dict
                if erros_acoes:
                    st.session_state.erros_gerais.extend(erros_acoes)

            if "Dividendos (B3)" in tipos_dados_selecionados:
                dividendos_temp = {}
                for ticker in tickers_list:
                    df_dividendos = buscar_dividendos_b3(ticker, df_empresas, data_inicio_dt, data_fim_dt)
                    if not df_dividendos.empty:
                        dividendos_temp[ticker] = df_dividendos
                st.session_state.todos_dados_dividendos = dividendos_temp

            if "Bonificações (B3)" in tipos_dados_selecionados:
                bonificacoes_temp = {}
                for ticker in tickers_list:
                    df_bonificacoes = buscar_bonificacoes_b3(ticker, df_empresas, data_inicio_dt, data_fim_dt)
                    if not df_bonificacoes.empty:
                        bonificacoes_temp[ticker] = df_bonificacoes
                st.session_state.todos_dados_bonificacoes = bonificacoes_temp
        
        st.session_state.dados_buscados = True
    else:
        st.warning("Por favor, preencha todos os campos: tickers, datas e selecione ao menos um tipo de dado.")

# --- EXIBIÇÃO E DOWNLOAD ---
if st.session_state.get('dados_buscados', False):
    if st.session_state.erros_gerais:
        for erro in st.session_state.erros_gerais:
            st.warning(erro)

    if st.session_state.todos_dados_acoes:
        st.subheader("1. Preços Históricos (Yahoo Finance)")
        df_acoes_agrupado = pd.concat(st.session_state.todos_dados_acoes.values(), ignore_index=True)
        st.dataframe(df_acoes_agrupado)

    if st.session_state.todos_dados_dividendos:
        st.subheader("2. Dividendos (B3)")
        df_dividendos_agrupado = pd.concat(st.session_state.todos_dados_dividendos.values(), ignore_index=True)
        st.dataframe(df_dividendos_agrupado)

    if st.session_state.todos_dados_bonificacoes:
        st.subheader("3. Bonificações (B3)")
        df_bonificacoes_agrupado = pd.concat(st.session_state.todos_dados_bonificacoes.values(), ignore_index=True)
        st.dataframe(df_bonificacoes_agrupado)

    if not st.session_state.todos_dados_acoes and not st.session_state.todos_dados_dividendos and not st.session_state.todos_dados_bonificacoes:
        st.info("Nenhum dado encontrado para os critérios selecionados.")
    else:
        st.subheader("📥 Download dos Dados em Excel")
        formato_excel = st.radio(
            "Escolha o formato do arquivo Excel:",
            ("Agrupar por tipo de dado (uma aba para Preços, outra para Dividendos, etc.)",
             "Separar por ticker e tipo (ex: Precos_PETR4, Div_VALE3, etc.)"),
            key="excel_format"
        )

        nome_arquivo = f"dados_mercado_{data_inicio_input.replace('/','')}_{data_fim_input.replace('/','')}_{datetime.now().strftime('%H%M')}.xlsx"
        try:
            from io import BytesIO
            output = BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                if formato_excel.startswith("Agrupar"):
                    if st.session_state.todos_dados_acoes:
                        pd.concat(st.session_state.todos_dados_acoes.values(), ignore_index=True).to_excel(writer, sheet_name="Precos_Historicos", index=False)
                    if st.session_state.todos_dados_dividendos:
                        pd.concat(st.session_state.todos_dados_dividendos.values(), ignore_index=True).to_excel(writer, sheet_name="Dividendos", index=False)
                    if st.session_state.todos_dados_bonificacoes:
                        pd.concat(st.session_state.todos_dados_bonificacoes.values(), ignore_index=True).to_excel(writer, sheet_name="Bonificacoes", index=False)
                
                else: # Separar por ticker e tipo
                    if st.session_state.todos_dados_acoes:
                        for ticker, df in st.session_state.todos_dados_acoes.items():
                            df.to_excel(writer, sheet_name=f"Precos_{ticker[:25]}", index=False)
                    if st.session_state.todos_dados_dividendos:
                        for ticker, df in st.session_state.todos_dados_dividendos.items():
                            df.to_excel(writer, sheet_name=f"Div_{ticker[:25]}", index=False)
                    if st.session_state.todos_dados_bonificacoes:
                        for ticker, df in st.session_state.todos_dados_bonificacoes.items():
                            df.to_excel(writer, sheet_name=f"Bonif_{ticker[:25]}", index=False)
            
            excel_data = output.getvalue()

            st.download_button(
                label="Baixar arquivo Excel",
                data=excel_data,
                file_name=nome_arquivo,
                mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
        except Exception as e:
            st.error(f"Erro ao gerar o arquivo Excel: {e}")

# --- Rodapé ---
st.markdown("""
---
**(Nota sobre Close vs. Adjusted Close: Historicamente usamos o preço de fechamento "Close" (não o "Adjusted Close"), pois é o valor bruto do dia, que historicamente se alinha mais com os dados extraídos diretamente da B3)**

**Fontes dos dados:**
- Preços Históricos: [Yahoo Finance](https://finance.yahoo.com)
- Dividendos e Eventos societários: [API B3](https://www.b3.com.br) 
""")
