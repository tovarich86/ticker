import streamlit as st
import requests
import pandas as pd
import yfinance as yf
from base64 import b64encode
from datetime import datetime, timedelta
import json

# URL do arquivo no GitHub
URL_EMPRESAS = "https://github.com/tovarich86/ticker/raw/refs/heads/main/empresas_b3.xlsx"

@st.cache_data
def carregar_empresas():
    try:
        df_empresas = pd.read_excel(URL_EMPRESAS)
        # Padronizar "Nome do Pregão"
        df_empresas['Nome do Pregão'] = df_empresas['Nome do Pregão'].str.replace(r'\s*S\.?A\.?', ' S.A.', regex=True).str.upper()
        return df_empresas
    except Exception as e:
        st.error(f"Erro ao carregar a planilha de empresas: {e}")
        return None

# Função para validar a data de entrada
def validar_data(data):
    try:
        return pd.to_datetime(datetime.strptime(data, '%d/%m/%Y').date())
    except ValueError:
        raise ValueError("Formato de data incorreto, deve ser DD/MM/AAAA")

# Função para buscar nome de pregão usando a API da B3
def get_trading_name(ticker, empresas_df):
    """
    Busca o nome de pregão de um ticker na planilha de empresas.
    Retorna None se o ticker não for encontrado.
    """
    for index, row in empresas_df.iterrows():
        tickers = [t.strip() for t in row['Tickers'].split(",")]
        if ticker in tickers:
            return row['Nome do Pregão']
    return None  # Retorna None se o ticker não for encontrado

def buscar_dividendos_b3(ticker, empresas_df, data_inicio, data_fim):
    """
    Retorna um DataFrame com dividendos do ticker em questão.
    Se não encontrar dividendos ou ocorrer erro, retorna DataFrame vazio.
    Tenta diferentes variações de "Nome do Pregão" se a busca inicial falhar.
    """
    trading_name_variations = []
    trading_name = get_trading_name(ticker, empresas_df)
    if trading_name:
        trading_name_variations = [trading_name,
                                   trading_name.replace(" SA", " S.A."),
                                   trading_name.replace(" SA", " S/A"),
                                   trading_name.replace(" SA", " SA.")]
    else:
        st.info(f"Ticker não encontrado: {ticker}")
        return pd.DataFrame()

    for trading_name in trading_name_variations:
        try:
            params = {
                "language": "pt-br",
                "pageNumber": "1",
                "pageSize": "60",
                "tradingName": trading_name,
            }
            # Converte o dicionário para JSON
            params_json = json.dumps(params)
            # Codifica o JSON para Base64
            params_encoded = b64encode(params_json.encode('ascii')).decode('ascii')
            url = f'https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetListedCashDividends/{params_encoded}'
            response = requests.get(url)
            response_json = response.json()

            if 'results' not in response_json:
                st.info(f'A chave "results" não está presente na resposta para o ticker {ticker} com nome de pregão "{trading_name}".')
                continue

            dividends_data = response_json['results']
            df = pd.DataFrame(dividends_data)
            df['Ticker'] = ticker  # Adiciona o Ticker como uma nova coluna

            # Reordenando as colunas para que 'Ticker' seja a primeira
            if 'Ticker' in df.columns:
                cols = ['Ticker'] + [col for col in df if col != 'Ticker']
                df = df[cols]

            # Imprime informações de debug
            st.write(f"Data de início: {data_inicio}")
            st.write(f"Data de fim: {data_fim}")

            # Converte 'dateApproval' para datetime e lida com valores ausentes
            df['dateApproval'] = pd.to_datetime(df['dateApproval'], format='%d/%m/%Y', errors='coerce')
            df = df.dropna(subset=['dateApproval'])

            # Imprime as datas após a conversão
            st.write("Datas após conversão:")
            st.write(df['dateApproval'])

            # Filtra o DataFrame pelo período
            df = df[(df['dateApproval'] >= data_inicio) & (df['dateApproval'] <= data_fim)]

            if not df.empty:
                return df  # Retorna o DataFrame se encontrar dividendos

        except Exception as e:
            st.info(f"Erro ao buscar dividendos para o ticker {ticker} com nome de pregão {trading_name}: {e}")

    st.info(f"Nenhum dividendo encontrado para o ticker {ticker} com as variações de nome de pregão consultadas.")
    return pd.DataFrame()  # Retorna DataFrame vazio se não encontrar em nenhuma variação

# Função para buscar dados históricos de ações via yfinance
def buscar_dados_acoes(tickers_input, data_inicio_input, data_fim_input):
    """
    Retorna dois dicionários:
      - dados_acoes_dict[ticker] = DataFrame com dados de cada ticker
      - erros (para logar e mostrar em st.info, se houver)
    """
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
                # Flatten do MultiIndex para evitar erros
                dados.columns = [col[0] if isinstance(col, tuple) else col for col in dados.columns]
                # Adicionando o ticker como coluna
                dados['Ticker'] = ticker
                # Transformando o índice de datas em uma coluna
                dados.reset_index(inplace=True)
                # Ajustando o formato da data
                dados['Date'] = dados['Date'].dt.strftime('%d/%m/%Y')

                # Armazenar este DataFrame no dicionário
                dados_acoes_dict[ticker] = dados
            else:
                erros.append(f"Sem dados para o ticker {ticker}")
        except Exception as e:
            erros.append(f"Erro ao buscar dados para {ticker}: {e}")
            continue

    return dados_acoes_dict, erros

# Interface do Streamlit
st.title('Consulta dados históricos de Ações e Dividendos')

# Carrega o DataFrame de empresas
df_empresas = carregar_empresas()

# Verifica se o DataFrame foi carregado corretamente
if df_empresas is None:
    st.error("Erro ao carregar a lista de empresas. Por favor, verifique a URL e tente novamente.")
    st.stop()  # Para a execução do script se não conseguir carregar a lista de empresas

# Entrada do usuário
tickers_input = st.text_input("Digite os tickers separados por vírgula (ex: PETR4, VALE3, ^BVSP):")
data_inicio_input = st.text_input("Digite a data de início (dd/mm/aaaa):")
data_fim_input = st.text_input("Digite a data de fim (dd/mm/aaaa):")
buscar_dividendos = st.checkbox("Adicionar os dividendos no período")

# Botão para buscar dados
if st.button('Buscar Dados'):
    if tickers_input and data_inicio_input and data_fim_input:
        # Validar as datas de entrada
        try:
            data_inicio = validar_data(data_inicio_input)
            data_fim = validar_data(data_fim_input)
        except ValueError as e:
            st.error(str(e))
            st.stop()

        # Obter a lista de tickers
        tickers = [ticker.strip() for ticker in tickers_input.split(',')]

        # Buscar dados de ações (cada ticker num DF separado)
        dados_acoes_dict, erros = buscar_dados_acoes(tickers_input, data_inicio_input, data_fim_input)

        # Exibir na tela possíveis erros
        for erro in erros:
            st.info(erro)

        # Caso não encontre dados de nenhum ticker
        if not dados_acoes_dict:
            st.info("Nenhum dado de ações encontrado para os tickers e período especificados.")
        else:
            st.write("### Dados de Ações por Ticker:")

            # Mostrar cada DataFrame de ação individualmente no Streamlit
            for ticker, df_acao in dados_acoes_dict.items():
                st.write(f"#### {ticker}")
                st.dataframe(df_acao)

            # -----------------------
            # DIVIDENDOS (opcional)
            # -----------------------
            dados_dividendos_dict = {}  # Inicializa o dicionário *fora* do loop
            if buscar_dividendos:
                for ticker in tickers:
                    df_dividendos = buscar_dividendos_b3(ticker, df_empresas, data_inicio, data_fim)
                    if not df_dividendos.empty:
                        dados_dividendos_dict[ticker] = df_dividendos  # Adiciona os dividendos ao dicionário... # Após buscar dividendos para todos os tickers, exibe os resultados
                if dados_dividendos_dict:  # Verifica se algum dividendo foi encontrado
                    st.write("### Dados de Dividendos por Ticker:")
                    for ticker, df_divid in dados_dividendos_dict.items():  # Itera sobre o dicionário de dividendos
                        st.write(f"#### {ticker}")
                        st.dataframe(df_divid)
                else:
                    st.info("Nenhum dado de dividendos encontrado para os tickers e período especificados.")

            # ------------------------------------------------
            # GERAR EXCEL: cada ticker em uma aba diferente
            # ------------------------------------------------
            nome_arquivo = "dados_acoes_dividendos_.xlsx"
            with pd.ExcelWriter(nome_arquivo) as writer:
                # 1) Gravar dados de ações (cada ticker em uma aba)
                for ticker, df_acao in dados_acoes_dict.items():
                    # sheet_name não pode ter mais de 31 caracteres no Excel,
                    # então podemos truncar ou simplesmente usar o ticker
                    sheet_name = f"Acoes_{ticker[:25]}"
                    df_acao.to_excel(writer, sheet_name=sheet_name, index=False)

                # 2) Se houver dados de dividendos, gravar por ticker também
                if buscar_dividendos and dados_dividendos_dict:
                    for ticker, df_divid in dados_dividendos_dict.items():
                        sheet_name = f"Div_{ticker[:25]}"
                        df_divid.to_excel(writer, sheet_name=sheet_name, index=False)

            # Botão de download do Excel
            with open(nome_arquivo, 'rb') as file:
                st.download_button(
                    label="Baixar arquivo Excel",
                    data=file,
                    file_name=nome_arquivo
                )
    else:
        st.error("Por favor, preencha todos os campos.")

st.markdown("""
---
**[[Fonte dos dados](pplx://action/followup):**
- Dados de ações obtidos de [Yahoo Finance](https://finance.yahoo.com)
- Dados de dividendos obtidos da [API da B3](https://www.b3.com.br)
- Código fonte [Github tovarich86](https://github.com/tovarich86/ticker)
""")
