import streamlit as st
import requests
import pandas as pd
import yfinance as yf
import base64
import json
from datetime import datetime, timedelta

# URL do arquivo no GitHub
URL_EMPRESAS = "https://github.com/tovarich86/ticker/raw/refs/heads/main/empresas_b3.xlsx"

@st.cache_data
def carregar_empresas():
    try:
        df_empresas = pd.read_excel(URL_EMPRESAS)
        return df_empresas
    except Exception as e:
        st.error(f"Erro ao carregar a planilha de empresas: {e}")
        return None

def get_trading_name(ticker, df_empresas):
    df_empresas['Tickers'] = df_empresas['Tickers'].astype(str).apply(lambda x: [t.strip() for t in x.split(",")])
    empresa = df_empresas[df_empresas['Tickers'].apply(lambda tickers: ticker in tickers)]
    if not empresa.empty:
        return empresa.iloc[0]['Nome do Pregão'].replace("/", "").replace(" ", "").upper()
    return None

def buscar_dividendos_b3(ticker, df_empresas):
    try:
        trading_name = get_trading_name(ticker, df_empresas)
        if not trading_name:
            return pd.DataFrame()
        
        payload = {
            "language": "pt-br",
            "pageNumber": 1,
            "pageSize": 99,
            "tradingName": trading_name
        }
        payload_encoded = base64.b64encode(json.dumps(payload).encode()).decode()
        url = f"https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetListedCashDividends/{payload_encoded}"
        
        response = requests.get(url)
        response_json = response.json()

        if 'results' not in response_json or not response_json['results']:
            return pd.DataFrame()

        df = pd.DataFrame(response_json['results'])
        df['Ticker'] = ticker
        df['dateApproval'] = pd.to_datetime(df['dateApproval'], errors='coerce')
        return df
    except Exception as e:
        print(f"Erro ao buscar dividendos para {ticker}: {e}")
        return pd.DataFrame()

def buscar_dados_acoes(tickers_input, data_inicio_input, data_fim_input):
    data_inicio = datetime.strptime(data_inicio_input, "%d/%m/%Y").strftime("%Y-%m-%d")
    data_fim = datetime.strptime(data_fim_input, "%d/%m/%Y").strftime("%Y-%m-%d")
    data_fim_ajustada = (datetime.strptime(data_fim, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")

    tickers = [
        ticker.strip() + '.SA' if any(char.isdigit() for char in ticker.strip()) and not ticker.strip().endswith('.SA')
        else ticker.strip()
        for ticker in tickers_input.split(",")
    ]

    dados_acoes_dict = {}
    erros = []

    for ticker in tickers:
        try:
            dados = yf.download(ticker, start=data_inicio, end=data_fim_ajustada, auto_adjust=False)
            if not dados.empty:
                dados.columns = [col[0] if isinstance(col, tuple) else col for col in dados.columns]
                dados['Ticker'] = ticker
                dados.reset_index(inplace=True)
                dados['Date'] = dados['Date'].dt.strftime('%d/%m/%Y')

                dados_acoes_dict[ticker] = dados
            else:
                erros.append(f"Sem dados para o ticker {ticker}")
        except Exception as e:
            erros.append(f"Erro ao buscar dados para {ticker}: {e}")
            continue

    return dados_acoes_dict, erros

st.title('Consulta dados históricos de Ações e Dividendos')

df_empresas = carregar_empresas()

tickers_input = st.text_input("Digite os tickers separados por vírgula (ex: PETR4, VALE3, ^BVSP):")
data_inicio_input = st.text_input("Digite a data de início (dd/mm/aaaa):")
data_fim_input = st.text_input("Digite a data de fim (dd/mm/aaaa):")
buscar_dividendos = st.checkbox("Adicionar os dividendos no período")

if st.button('Buscar Dados'):
    if tickers_input and data_inicio_input and data_fim_input:
        if df_empresas is not None:
            tickers = [t.strip() for t in tickers_input.split(',')]
            data_inicio = pd.to_datetime(data_inicio_input, format='%d/%m/%Y')
            data_fim = pd.to_datetime(data_fim_input, format='%d/%m/%Y')
            dados_acoes_dict, erros = buscar_dados_acoes(tickers_input, data_inicio_input, data_fim_input)
            dados_dividendos_dict = {}

            for erro in erros:
                st.info(erro)

            if dados_acoes_dict:
                st.write("### Dados de Ações por Ticker:")
                for ticker, df_acao in dados_acoes_dict.items():
                    st.write(f"#### {ticker}")
                    st.dataframe(df_acao)

            if buscar_dividendos:
                for ticker in tickers:
                    df_dividendos = buscar_dividendos_b3(ticker, df_empresas)
                    if not df_dividendos.empty:
                        df_dividendos['dateApproval'] = pd.to_datetime(df_dividendos['dateApproval'], errors='coerce')
                        df_dividendos = df_dividendos[
                            (df_dividendos['dateApproval'] >= data_inicio) &
                            (df_dividendos['dateApproval'] <= data_fim)
                        ]
                        if not df_dividendos.empty:
                            dados_dividendos_dict[ticker] = df_dividendos

            if dados_dividendos_dict:
                st.write("### Dados de Dividendos por Ticker:")
                for ticker, df_divid in dados_dividendos_dict.items():
                    st.write(f"#### {ticker}")
                    st.dataframe(df_divid)

            nome_arquivo = "dados_acoes_dividendos.xlsx"
            with pd.ExcelWriter(nome_arquivo) as writer:
                for ticker, df_acao in dados_acoes_dict.items():
                    df_acao.to_excel(writer, sheet_name=f"Acoes_{ticker[:25]}", index=False)

                if buscar_dividendos and dados_dividendos_dict:
                    for ticker, df_divid in dados_dividendos_dict.items():
                        df_divid.to_excel(writer, sheet_name=f"Div_{ticker[:25]}", index=False)

            with open(nome_arquivo, 'rb') as file:
                st.download_button(
                    label="Baixar arquivo Excel", 
                    data=file, 
                    file_name=nome_arquivo
                )
    else:
        st.error("Por favor, preencha todos os campos.")

st.markdown("**Fonte dos dados:** Yahoo Finance, API da B3")
