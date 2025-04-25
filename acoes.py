# -*- coding: utf-8 -*-
import streamlit as st
import requests
import pandas as pd
import yfinance as yf
from base64 import b64encode
from datetime import datetime, timedelta
import json
import re
import time # Importar para usar time.sleep
import traceback # Para mostrar erros detalhados
from io import BytesIO # Para Excel em memória

# URL do arquivo no GitHub
URL_EMPRESAS = "https://github.com/tovarich86/ticker/raw/refs/heads/main/empresas_b3.xlsx"

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
    # Iterar sobre as linhas do DataFrame filtrado para maior eficiência
    filtered_df = empresas_df[empresas_df['Tickers'].str.contains(ticker_upper, case=False, na=False)]
    for index, row in filtered_df.iterrows():
        # Divide a string de tickers, remove espaços e converte para maiúsculas
        tickers_list = [t.strip().upper() for t in row['Tickers'].split(",") if t.strip()]
        if ticker_upper in tickers_list:
            return {
                'trading_name': row['Nome do Pregão'],
                'code': row['CODE'],
                'type_stock': row['typeStock']
            }
    return None  # Retorna None se o ticker não for encontrado

# --- Função de Busca de Dividendos (com Paginação e Filtro typeStock) ---
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
        st.warning(f"Info não encontrada para {ticker} na planilha. Dividendos B3 não buscados.")
        return pd.DataFrame()

    trading_name = ticker_info.get('trading_name')
    desired_type_stock = ticker_info.get('type_stock')

    if not trading_name:
         st.warning(f"Nome pregão não encontrado para {ticker}. Dividendos B3 não buscados.")
         return pd.DataFrame()
    if not desired_type_stock:
        st.warning(f"typeStock não encontrado para {ticker}. Não é possível filtrar dividendos B3.")
        return pd.DataFrame()

    all_dividends = []
    current_page = 1
    total_pages = 1
    api_called = False

    while current_page <= total_pages:
        try:
            api_called = True
            params = {
                "language": "pt-br",
                "pageNumber": str(current_page),
                "pageSize": "50",
                "tradingName": trading_name,
            }
            params_json = json.dumps(params)
            params_encoded = b64encode(params_json.encode('utf-8')).decode('utf-8')
            url = f'https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetListedCashDividends/{params_encoded}'

            response = requests.get(url, timeout=30)
            response.raise_for_status()

            if not response.content or not response.text.strip():
                 if current_page == 1: pass
                 break

            try:
                response_json = response.json()
            except json.JSONDecodeError:
                st.error(f"Erro JSON dividendos B3 para {ticker} (pág {current_page}).")
                break

            if current_page == 1 and 'page' in response_json and 'totalPages' in response_json['page']:
                total_pages = int(response_json['page']['totalPages'])

            if 'results' in response_json and response_json['results']:
                all_dividends.extend(response_json['results'])
            elif current_page == 1:
                 break

            if total_pages > 1 and current_page < total_pages:
                 time.sleep(0.3)

            current_page += 1

        except requests.exceptions.RequestException as e:
            st.error(f"Erro rede dividendos B3 para {ticker} (pág {current_page}): {e}")
            break
        except Exception as e:
            st.error(f"Erro inesperado dividendos B3 para {ticker} (pág {current_page}): {e}")
            break

    if not all_dividends:
        return pd.DataFrame()

    df = pd.DataFrame(all_dividends)

    if 'typeStock' in df.columns:
         df['typeStock'] = df['typeStock'].astype(str).str.strip().str.upper()
         df = df[df['typeStock'] == desired_type_stock].copy()
         if df.empty: return pd.DataFrame()
    else:
         st.warning(f"Coluna 'typeStock' não encontrada nos dividendos B3 para {ticker}.")

    df['Ticker'] = ticker

    if 'lastDatePriorEx' in df.columns:
        df['lastDatePriorEx_dt'] = pd.to_datetime(df['lastDatePriorEx'], format='%d/%m/%Y', errors='coerce')
        df = df.dropna(subset=['lastDatePriorEx_dt'])
        df = df[(df['lastDatePriorEx_dt'] >= data_inicio) & (df['lastDatePriorEx_dt'] <= data_fim)]
        df = df.drop(columns=['lastDatePriorEx_dt'])
    else:
        st.warning(f"Coluna 'lastDatePriorEx' não encontrada dividendos B3 {ticker}.")
        return pd.DataFrame()

    if df.empty: return pd.DataFrame()

    cols = ['Ticker'] + [col for col in df.columns if col != 'Ticker']
    df = df[cols]

    return df

# --- Função de Busca de Eventos Societários (Bonificações) ---
def buscar_eventos_societarios_b3(ticker, empresas_df, data_inicio, data_fim):
    """Busca eventos societários (foco em bonificações/'stockDividends') na B3 usando o CODE."""
    if not any(char.isdigit() for char in ticker):
        return pd.DataFrame()

    ticker_info = get_ticker_info(ticker, empresas_df)
    if not ticker_info or not ticker_info.get('code'):
        st.warning(f"CODE não encontrado para {ticker}. Eventos B3 não buscados.")
        return pd.DataFrame()

    code = ticker_info['code']
    api_called = False

    try:
        api_called = True
        params_eventos = {
            "issuingCompany": code,
            "language": "pt-br"
        }
        params_json = json.dumps(params_eventos)
        params_encoded = b64encode(params_json.encode('utf-8')).decode('utf-8')
        url = f'https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetListedSupplementCompany/{params_encoded}'

        response = requests.get(url, timeout=30)
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
        if df.empty: return pd.DataFrame()

        df['Ticker'] = ticker
        if 'lastDatePrior' in df.columns:
             df['lastDatePrior_dt'] = pd.to_datetime(df['lastDatePrior'], format='%d/%m/%Y', errors='coerce')
             df = df.dropna(subset=['lastDatePrior_dt'])
             df = df[(df['lastDatePrior_dt'] >= data_inicio) & (df['lastDatePrior_dt'] <= data_fim)]
             df = df.drop(columns=['lastDatePrior_dt'])
        else:
             st.warning(f"Coluna 'lastDatePrior' não encontrada eventos B3 {ticker}.")
             return pd.DataFrame()

        if df.empty: return pd.DataFrame()

        cols = ['Ticker'] + [col for col in df.columns if col != 'Ticker']
        df = df[cols]

        return df

    except requests.exceptions.RequestException as e:
        st.error(f"Erro rede eventos B3 para {ticker} (CODE: {code}): {e}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Erro inesperado eventos B3 para {ticker} (CODE: {code}): {e}")
        return pd.DataFrame()


# --- Função para buscar dados históricos de ações via yfinance ---
def buscar_dados_acoes(tickers_input, data_inicio_input, data_fim_input):
    """Busca dados históricos de preços de ações usando yfinance."""
    try:
        data_inicio_dt = datetime.strptime(data_inicio_input, "%d/%m/%Y")
        data_fim_dt = datetime.strptime(data_fim_input, "%d/%m/%Y")
        data_inicio_str = data_inicio_dt.strftime("%Y-%m-%d")
        data_fim_ajustada_str = (data_fim_dt + timedelta(days=1)).strftime("%Y-%m-%d")
    except ValueError:
        st.error("Formato de data inválido para preços. Use dd/mm/aaaa.")
        return {}, ["Formato de data inválido."]

    tickers_list = [ticker.strip().upper() for ticker in tickers_input.split(',') if ticker.strip()]
    dados_acoes_dict = {}
    erros = []

    for ticker in tickers_list:
        ticker_yf = ticker
        if any(char.isdigit() for char in ticker) and not ticker.endswith('.SA'):
             ticker_yf = ticker + '.SA'

        try:
            # Usando multi_level_index=False
            dados = yf.download(ticker_yf, start=data_inicio_str, end=data_fim_ajustada_str,
                                auto_adjust=False, progress=False,
                                multi_level_index=False)

            if not dados.empty:
                dados.reset_index(inplace=True)
                dados['Date'] = pd.to_datetime(dados['Date'])
                dados = dados[(dados['Date'] >= data_inicio_dt) & (dados['Date'] <= data_fim_dt)]

                if dados.empty: continue

                dados['Date'] = dados['Date'].dt.strftime('%d/%m/%Y')
                dados['Ticker'] = ticker
                cols = ['Ticker', 'Date'] + [col for col in dados.columns if col not in ['Ticker', 'Date']]
                dados = dados[cols]
                dados_acoes_dict[ticker] = dados
            else:
                 erros.append(f"Sem dados de preços (yfinance) encontrados para {ticker} ({ticker_yf}).")

        except Exception as e:
            erros.append(f"Erro ao buscar preços (yfinance) para {ticker} ({ticker_yf}): {e}")
            continue

    return dados_acoes_dict, erros

# ============================================
# Interface do Streamlit
# ============================================
st.set_page_config(layout="wide")
st.title('Consulta Dados de Mercado B3 e Yahoo Finance')

df_empresas = carregar_empresas()
if df_empresas.empty:
    st.error("Falha ao carregar lista de empresas. Verifique URL/arquivo. Aplicação interrompida.")
    st.stop()

col1, col2 = st.columns(2)
with col1:
    tickers_input = st.text_input("Tickers (separados por vírgula):", key="tickers", placeholder="Ex: PETR4, VALE3, MGLU3")
with col2:
    tipos_dados_selecionados = st.multiselect(
        "Selecione os dados:",
        ["Preços(YFinance)", "Dividendos (B3)", "Eventos societários (B3)"],
        default=["Preços(YFinance)"],
        key="data_types"
    )

col3, col4 = st.columns(2)
today_str = datetime.now().strftime("%d/%m/%Y")
last_year_str = (datetime.now() - timedelta(days=365)).strftime("%d/%m/%Y")
with col3:
    data_inicio_input = st.text_input("Data de início (dd/mm/aaaa):", key="date_start", value=last_year_str)
with col4:
    data_fim_input = st.text_input("Data de fim (dd/mm/aaaa):", key="date_end", value=today_str)

if st.button('Buscar Dados', key="search_button"):
    if not tickers_input or not data_inicio_input or not data_fim_input or not tipos_dados_selecionados:
        st.warning("Preencha todos os campos: Tickers, Datas e selecione ao menos um Tipo de Dado.")
        st.stop()

    try:
        data_inicio_dt = datetime.strptime(data_inicio_input, "%d/%m/%Y")
        data_fim_dt = datetime.strptime(data_fim_input, "%d/%m/%Y")
        if data_inicio_dt > data_fim_dt:
             st.error("Data de início não pode ser posterior à data de fim.")
             st.stop()
    except ValueError:
        st.error("Formato de data inválido. Use dd/mm/aaaa.")
        st.stop()

    tickers_list = sorted(list(set([ticker.strip().upper() for ticker in tickers_input.split(',') if ticker.strip()])))
    if not tickers_list:
        st.warning("Nenhum ticker válido fornecido.")
        st.stop()

    todos_dados_precos = {}
    todos_dados_dividendos = {}
    todos_dados_eventos = {}
    erros_gerais = []

    progress_bar = st.progress(0)
    status_text = st.empty()
    total_steps = 0
    if "Preços(YFinance)" in tipos_dados_selecionados: total_steps += len(tickers_list)
    if "Dividendos (B3)" in tipos_dados_selecionados: total_steps += len(tickers_list)
    if "Eventos societários (B3)" in tipos_dados_selecionados: total_steps += len(tickers_list)
    
    current_step = 0 # Definido no mesmo escopo da função abaixo

    def update_progress(steps_done=1):
        # nonlocal current_step # <<< REMOVIDA
        nonlocal current_step # Correção: Necessário para modificar a variável do escopo externo
        current_step += steps_done
        if total_steps > 0:
            progress_bar.progress(min(current_step / total_steps, 1.0))

    with st.spinner('Buscando dados...'):
        if "Preços(YFinance)" in tipos_dados_selecionados:
            status_text.text(f"Buscando Preços Históricos (Yahoo Finance)...")
            dados_acoes_dict, erros_acoes = buscar_dados_acoes(tickers_input, data_inicio_input, data_fim_input)
            if dados_acoes_dict: todos_dados_precos = dados_acoes_dict
            if erros_acoes: erros_gerais.extend(erros_acoes)
            update_progress(len(tickers_list))

        if "Dividendos (B3)" in tipos_dados_selecionados:
            for i, ticker in enumerate(tickers_list):
                 status_text.text(f"Buscando Dividendos (B3) para {ticker} ({i+1}/{len(tickers_list)})...")
                 df_dividendos = buscar_dividendos_b3(ticker, df_empresas, data_inicio_dt, data_fim_dt)
                 if not df_dividendos.empty: todos_dados_dividendos[ticker] = df_dividendos
                 update_progress()

        if "Eventos societários (B3)" in tipos_dados_selecionados:
            for i, ticker in enumerate(tickers_list):
                 status_text.text(f"Buscando Eventos Societários (B3) para {ticker} ({i+1}/{len(tickers_list)})...")
                 df_eventos = buscar_eventos_societarios_b3(ticker, df_empresas, data_inicio_dt, data_fim_dt)
                 if not df_eventos.empty: todos_dados_eventos[ticker] = df_eventos
                 update_progress()

    status_text.text("Busca concluída!")
    progress_bar.empty()

    st.markdown("---")
    dados_exibidos = False

    if "Preços(YFinance)" in tipos_dados_selecionados:
        st.subheader("1. Preços Históricos (Yahoo Finance)")
        if todos_dados_precos:
             df_precos_agrupado = pd.concat(todos_dados_precos.values(), ignore_index=True)
             st.dataframe(df_precos_agrupado)
             dados_exibidos = True
        elif not any("preços (yfinance)" in e.lower() for e in erros_gerais):
             st.info("Nenhum dado de preço histórico encontrado para os tickers/período.")

    if "Dividendos (B3)" in tipos_dados_selecionados:
        st.subheader("2. Dividendos (B3)")
        if todos_dados_dividendos:
             df_dividendos_agrupado = pd.concat(todos_dados_dividendos.values(), ignore_index=True)
             st.dataframe(df_dividendos_agrupado)
             dados_exibidos = True
        else:
             st.info("Nenhum dado de dividendo encontrado na B3 para os tickers/período/tipo de ação especificados.")

    if "Eventos societários (B3)" in tipos_dados_selecionados:
        st.subheader("3. Eventos Societários (B3)")
        if todos_dados_eventos:
            df_eventos_agrupado = pd.concat(todos_dados_eventos.values(), ignore_index=True)
            st.dataframe(df_eventos_agrupado)
            dados_exibidos = True
        else:
            st.info("Nenhum evento societário (bonificação) encontrado na B3 para os tickers/período especificados.")

    if erros_gerais:
       st.subheader("⚠️ Avisos e Erros")
       for erro in erros_gerais:
           st.warning(erro)

    if dados_exibidos:
        st.subheader("📥 Download dos Dados")
        formato_excel = st.radio(
            "Escolha o formato do Excel:",
            ("Agrupar por tipo de dado", "Uma aba por ticker/tipo"),
            key="excel_format"
        )

        nome_arquivo = f"dados_mercado_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        try:
            output = BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                if formato_excel == "Agrupar por tipo de dado":
                    if todos_dados_precos:
                        pd.concat(todos_dados_precos.values(), ignore_index=True).to_excel(writer, sheet_name="Precos_YFinance", index=False)
                    if todos_dados_dividendos:
                        pd.concat(todos_dados_dividendos.values(), ignore_index=True).to_excel(writer, sheet_name="Dividendos", index=False)
                    if todos_dados_eventos:
                        pd.concat(todos_dados_eventos.values(), ignore_index=True).to_excel(writer, sheet_name="Eventos_Societarios", index=False)
                else:
                    if todos_dados_precos:
                        for ticker, df_acao in todos_dados_precos.items():
                            sheet_name = re.sub(r'[\[\]\*:\\\?\/]', '', f"Precos_{ticker}")[:31]
                            df_acao.to_excel(writer, sheet_name=sheet_name, index=False)
                    if todos_dados_dividendos:
                        for ticker, df_divid in todos_dados_dividendos.items():
                            sheet_name = re.sub(r'[\[\]\*:\\\?\/]', '', f"Div_{ticker}")[:31]
                            df_divid.to_excel(writer, sheet_name=sheet_name, index=False)
                    if todos_dados_eventos:
                        for ticker, df_ev in todos_dados_eventos.items():
                            sheet_name = re.sub(r'[\[\]\*:\\\?\/]', '', f"Eventos_{ticker}")[:31]
                            df_ev.to_excel(writer, sheet_name=sheet_name, index=False)

            st.download_button(
                label="Baixar arquivo Excel",
                data=output.getvalue(),
                file_name=nome_arquivo,
                mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
        except Exception as e:
             st.error(f"Erro ao gerar o arquivo Excel: {e}")
             st.error(traceback.format_exc())

    elif not erros_gerais:
         st.info("Nenhum dado encontrado para os critérios selecionados.")

st.markdown("""
---
**Fontes:** Yahoo Finance (Preços), API B3 (Dividendos, Eventos). Mapeamento via Excel externo.
Código base por [tovarich86](https://github.com/tovarich86/ticker), modificado.
""")
