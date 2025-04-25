import streamlit as st
import requests
import pandas as pd
import yfinance as yf
from base64 import b64encode
from datetime import datetime, timedelta
import json
import re
import time # Importar para usar time.sleep

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

# --- Função de Busca de Dividendos (com Paginação e Filtro typeStock) ---
def buscar_dividendos_b3(ticker, empresas_df, data_inicio, data_fim):
    """
    Busca dividendos na B3 para um ticker específico, tratando paginação
    e filtrando pelo typeStock correto (ON, PN, UNT).
    Retorna um DataFrame com os dividendos filtrados ou DataFrame vazio.
    """
    if not any(char.isdigit() for char in ticker):
        # st.info(f"Ticker {ticker}: Parece internacional, buscando apenas em yfinance.")
        return pd.DataFrame()

    ticker_info = get_ticker_info(ticker, empresas_df)

    if not ticker_info:
        st.warning(f"Informações não encontradas para o ticker {ticker} na planilha de empresas.")
        return pd.DataFrame()

    trading_name = ticker_info['trading_name']
    desired_type_stock = ticker_info['type_stock'] # Tipo de ação (ON, PN, UNT) do ticker buscado

    if not trading_name:
         st.warning(f"Nome de pregão não encontrado para o ticker {ticker}.")
         return pd.DataFrame()
    if not desired_type_stock:
        st.warning(f"Tipo de ação (typeStock) não encontrado para o ticker {ticker} na planilha.")
        # Pode-se optar por continuar sem filtrar ou retornar vazio. Vamos retornar vazio por segurança.
        return pd.DataFrame()

    all_dividends = []
    current_page = 1
    total_pages = 1 # Inicializa com 1 para fazer a primeira requisição

    st.write(f"Buscando dividendos para {ticker} ({trading_name}, Tipo: {desired_type_stock})...")

    while current_page <= total_pages:
        try:
            params = {
                "language": "pt-br",
                "pageNumber": str(current_page),
                "pageSize": "50", # Ajustado para um tamanho razoável
                "tradingName": trading_name,
                # Não incluimos typeStock aqui, pois a API parece não suportar; filtramos depois
            }
            params_json = json.dumps(params)
            params_encoded = b64encode(params_json.encode('utf-8')).decode('utf-8') # Usar utf-8
            url = f'https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetListedCashDividends/{params_encoded}'

            response = requests.get(url, timeout=30) # Adiciona timeout
            response.raise_for_status() # Levanta erro para status >= 400
            response_json = response.json()

            # Atualiza o total de páginas na primeira requisição bem-sucedida
            if current_page == 1 and 'page' in response_json and 'totalPages' in response_json['page']:
                total_pages = int(response_json['page']['totalPages'])
                st.write(f"Total de {total_pages} páginas de dividendos encontradas para {trading_name}.")


            if 'results' in response_json and response_json['results']:
                all_dividends.extend(response_json['results'])
            elif current_page == 1:
                 # st.info(f"Nenhum dividendo encontrado na B3 para {ticker} ({trading_name}) na página {current_page}.")
                 break # Sai se não houver resultados na primeira página

            # Pausa leve para evitar sobrecarregar a API
            if total_pages > 1:
                 time.sleep(0.5) # Pausa de 0.5 segundos entre páginas

            current_page += 1

        except requests.exceptions.RequestException as e:
            st.error(f"Erro de rede ao buscar dividendos para {ticker} (página {current_page}): {e}")
            # Decide se quer tentar novamente ou parar
            break # Para em caso de erro de rede
        except json.JSONDecodeError:
             st.error(f"Erro ao decodificar JSON da resposta da B3 para {ticker} (página {current_page}).")
             break # Para se a resposta não for JSON válido
        except Exception as e:
            st.error(f"Erro inesperado ao buscar dividendos para {ticker} (página {current_page}): {e}")
            break # Para em caso de outros erros

    if not all_dividends:
        # st.info(f"Nenhum dividendo encontrado na B3 para {ticker} ({trading_name}) após consulta.")
        return pd.DataFrame()

    # Criar DataFrame com todos os resultados
    df = pd.DataFrame(all_dividends)

    # --- Filtragem pós-busca ---
    # 1. Filtrar pelo typeStock desejado
    if 'typeStock' in df.columns:
         df['typeStock'] = df['typeStock'].str.strip().str.upper() # Limpa e padroniza
         df_filtered_type = df[df['typeStock'] == desired_type_stock].copy() # Filtra pelo tipo correto
         if df_filtered_type.empty:
              # st.info(f"Dividendos encontrados para {trading_name}, mas nenhum do tipo {desired_type_stock} para o ticker {ticker}.")
              return pd.DataFrame()
         df = df_filtered_type
    else:
         st.warning(f"Coluna 'typeStock' não encontrada nos resultados da B3 para {ticker}. Não foi possível filtrar por tipo de ação.")
         # Decide se continua sem filtro ou retorna vazio. Vamos continuar sem filtro neste caso.

    # 2. Adicionar coluna Ticker
    df['Ticker'] = ticker

    # 3. Converter datas e filtrar pelo período
    if 'lastDatePriorEx' in df.columns:
        df['lastDatePriorEx'] = pd.to_datetime(df['lastDatePriorEx'], format='%d/%m/%Y', errors='coerce')
        df = df.dropna(subset=['lastDatePriorEx'])
        df = df[(df['lastDatePriorEx'] >= data_inicio) & (df['lastDatePriorEx'] <= data_fim)]
    else:
        st.warning(f"Coluna 'lastDatePriorEx' não encontrada para filtrar datas de dividendos de {ticker}.")
        return pd.DataFrame() # Retorna vazio se não puder filtrar por data

    # Reordenar colunas
    if 'Ticker' in df.columns:
        cols = ['Ticker'] + [col for col in df if col != 'Ticker']
        df = df[cols]

    if df.empty:
       # st.info(f"Nenhum dividendo encontrado para {ticker} (Tipo: {desired_type_stock}) no período selecionado.")
       pass # Não mostra info se já mostrou antes

    return df

# --- Função de Busca de cações (Eventos Societários) ---
# Renomeada para clareza
def buscar_cacoes_b3(ticker, empresas_df, data_inicio, data_fim):
    """Busca eventos de cação (stock dividends) na B3 usando o CODE da empresa."""
    if not any(char.isdigit() for char in ticker):
        # st.info(f"Ticker {ticker}: Parece internacional, eventos societários da B3 não serão buscadas.")
        return pd.DataFrame()

    ticker_info = get_ticker_info(ticker, empresas_df)
    if not ticker_info or not ticker_info.get('code'):
        st.warning(f"Código (CODE) não encontrado para o ticker {ticker} na planilha. Não é possível buscar bonificações.")
        return pd.DataFrame()

    code = ticker_info['code']

    try:
        params_bonificacoes = {
            "issuingCompany": code,
            "language": "pt-br"
        }
        params_json = json.dumps(params_bonificacoes)
        params_encoded = b64encode(params_json.encode('utf-8')).decode('utf-8')
        url = f'https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetListedSupplementCompany/{params_encoded}'

        response = requests.get(url, timeout=30)
        response.raise_for_status()

        # Verifica se a resposta é válida antes de tentar decodificar JSON
        if not response.content or not response.text.strip():
             # st.info(f"Resposta vazia da API de bonificações para {ticker} (Código: {code}).")
             return pd.DataFrame()
        try:
            data = response.json()
        except json.JSONDecodeError:
             # st.info(f"Resposta inválida (não JSON) da API de bonificações para {ticker} (Código: {code}).")
             return pd.DataFrame()


        # Verifica a estrutura esperada da resposta
        if not isinstance(data, list) or not data or "stockDividends" not in data[0] or not data[0]["stockDividends"]:
            # st.info(f"Nenhum dado de bonificação ('stockDividends') encontrado na resposta para {ticker} (Código: {code}).")
            return pd.DataFrame()

        df = pd.DataFrame(data[0]["stockDividends"])
        if df.empty:
            return pd.DataFrame()

        # Adiciona Ticker e filtra por data
        df['Ticker'] = ticker
        if 'lastDatePrior' in df.columns:
             df['lastDatePrior'] = pd.to_datetime(df['lastDatePrior'], format='%d/%m/%Y', errors='coerce')
             df = df.dropna(subset=['lastDatePrior'])
             df = df[(df['lastDatePrior'] >= data_inicio) & (df['lastDatePrior'] <= data_fim)]
        else:
             st.warning(f"Coluna 'lastDatePrior' não encontrada para filtrar datas de bonificações de {ticker}.")
             return pd.DataFrame() # Retorna vazio se não puder filtrar data


        # Reordena colunas
        if 'Ticker' in df.columns:
                cols = ['Ticker'] + [col for col in df if col != 'Ticker']
                df = df[cols]


        return df

    except requests.exceptions.RequestException as e:
        st.error(f"Erro de rede ao buscar bonificações para {ticker} (Código: {code}): {e}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Erro inesperado ao buscar bonificações para {ticker} (Código: {code}): {e}")
        return pd.DataFrame()


# --- Função para buscar dados históricos de ações via yfinance (mantida como antes) ---
def buscar_dados_acoes(tickers_input, data_inicio_input, data_fim_input):
    """Busca dados históricos de preços de ações usando yfinance."""
    try:
        data_inicio = datetime.strptime(data_inicio_input, "%d/%m/%Y").strftime("%Y-%m-%d")
        data_fim = datetime.strptime(data_fim_input, "%d/%m/%Y")
        data_fim_ajustada = (data_fim + timedelta(days=1)).strftime("%Y-%m-%d") # Para incluir a data fim
    except ValueError:
        st.error("Formato de data inválido. Use dd/mm/aaaa.")
        return {}, ["Formato de data inválido."]

    tickers_list = [ticker.strip().upper() for ticker in tickers_input.split(',') if ticker.strip()]
    dados_acoes_dict = {}
    erros = []

    for ticker in tickers_list:
        ticker_yf = ticker # Ticker base
        # Adiciona '.SA' para tickers brasileiros (regra simples: contém número)
        if any(char.isdigit() for char in ticker) and not ticker.endswith('.SA'):
             ticker_yf = ticker + '.SA'

        try:
            st.write(f"Buscando preços históricos para {ticker} ({ticker_yf})...")
            dados = yf.download(ticker_yf, start=data_inicio, end=data_fim_ajustada, auto_adjust=False, progress=False) # auto_adjust=False pode ser importante

            if not dados.empty:
                # Resetar índice para ter 'Date' como coluna
                dados.reset_index(inplace=True)
                # Formatar Data para dd/mm/aaaa
                dados['Date'] = pd.to_datetime(dados['Date']).dt.strftime('%d/%m/%Y')
                # Adicionar coluna Ticker (original, sem .SA)
                dados['Ticker'] = ticker
                # Reordenar para Ticker ser a primeira coluna
                cols = ['Ticker', 'Date'] + [col for col in dados.columns if col not in ['Ticker', 'Date']]
                dados = dados[cols]
                # Remover linhas onde a data está fora do período original (yf pode trazer dias extras)
                dados['Date_dt'] = pd.to_datetime(dados['Date'], format='%d/%m/%Y')
                dados = dados[dados['Date_dt'] <= data_fim]
                dados = dados.drop(columns=['Date_dt'])

                dados_acoes_dict[ticker] = dados
            else:
                erros.append(f"Sem dados de preços históricos encontrados para o ticker {ticker} ({ticker_yf}) no período.")
        except Exception as e:
            erros.append(f"Erro ao buscar dados de preços para {ticker} ({ticker_yf}): {e}")
            continue

    return dados_acoes_dict, erros

# ============================================
# Interface do Streamlit
# ============================================
st.set_page_config(layout="wide") # Usa layout largo
st.title('Consulta Dados de Mercado B3 e Yahoo Finance')

# --- Carrega o DataFrame de empresas ---
df_empresas = carregar_empresas()

if df_empresas.empty:
    st.error("Não foi possível carregar a lista de empresas. Verifique a URL ou o arquivo. A aplicação não pode continuar.")
    st.stop()
# else:
    # st.success(f"{len(df_empresas)} empresas carregadas com sucesso.")
    # Opcional: Mostrar uma prévia ou informações sobre o df_empresas
    # st.dataframe(df_empresas.head())


# --- Entradas do Usuário ---
col1, col2 = st.columns(2)
with col1:
    tickers_input = st.text_input("Digite os tickers separados por vírgula (ex: PETR4, VALE3, MGLU3, ITUB4):", key="tickers")
with col2:
    # Seleção dos tipos de dados a buscar
    tipos_dados_selecionados = st.multiselect(
        "Selecione os dados que deseja buscar:",
        ["Preços(Yahoo Finance)", "Dividendos (B3)", "Eventos societários (B3)"],
        default=["Preços(Yahoo Finance)"], # Padrão
        key="data_types"
    )

col3, col4 = st.columns(2)
with col3:
    data_inicio_input = st.text_input("Data de início (dd/mm/aaaa):", key="date_start")
with col4:
    data_fim_input = st.text_input("Data de fim (dd/mm/aaaa):", key="date_end")


# --- Botão e Lógica Principal ---
if st.button('Buscar Dados', key="search_button"):
    if tickers_input and data_inicio_input and data_fim_input and tipos_dados_selecionados:
        # Validar formato das datas
        try:
            data_inicio_dt = datetime.strptime(data_inicio_input, "%d/%m/%Y")
            data_fim_dt = datetime.strptime(data_fim_input, "%d/%m/%Y")
            if data_inicio_dt > data_fim_dt:
                 st.error("A data de início não pode ser posterior à data de fim.")
                 st.stop()
        except ValueError:
            st.error("Formato de data inválido. Use dd/mm/aaaa.")
            st.stop()

        # Limpa e obtém a lista de tickers únicos
        tickers_list = sorted(list(set([ticker.strip().upper() for ticker in tickers_input.split(',') if ticker.strip()])))

        # Dicionários para armazenar os resultados por tipo de dado
        todos_dados_acoes = {}
        todos_dados_dividendos = {}
        todos_dados_bonificacoes = {}
        erros_gerais = []

        # --- Busca de Dados ---
        with st.spinner('Buscando dados... Por favor, aguarde.'):
            # 1. Preços Históricos
            if "Preços Históricos (Yahoo Finance)" in tipos_dados_selecionados:
                st.subheader("1. Preços Históricos (Yahoo Finance)")
                dados_acoes_dict, erros_acoes = buscar_dados_acoes(tickers_input, data_inicio_input, data_fim_input)
                if dados_acoes_dict:
                    todos_dados_acoes = dados_acoes_dict
                    for ticker, df_acao in todos_dados_acoes.items():
                        st.write(f"**{ticker}**")
                        st.dataframe(df_acao)
                if erros_acoes:
                    erros_gerais.extend(erros_acoes)
                    for erro in erros_acoes:
                        st.warning(erro) # Usar warning para erros não críticos
                if not dados_acoes_dict and not erros_acoes:
                     st.info("Nenhum dado de preço histórico encontrado para os tickers/período.")


            # 2. Dividendos
            if "Dividendos (B3)" in tipos_dados_selecionados:
                st.subheader("2. Dividendos (B3)")
                dividendos_encontrados_algum_ticker = False
                for ticker in tickers_list:
                     # st.write(f"Processando dividendos para: {ticker}") # Feedback
                     df_dividendos = buscar_dividendos_b3(ticker, df_empresas, data_inicio_dt, data_fim_dt)
                     if not df_dividendos.empty:
                         todos_dados_dividendos[ticker] = df_dividendos
                         dividendos_encontrados_algum_ticker = True
                         # st.write(f"**{ticker}**") # Não mostra mais individualmente aqui, agrupa no final
                         # st.dataframe(df_dividendos) # Não mostra mais individualmente aqui

                if dividendos_encontrados_algum_ticker:
                    st.write("Dividendos encontrados:")
                    # Exibe todos os dataframes de dividendos concatenados ou por ticker
                    df_dividendos_agrupado = pd.concat(todos_dados_dividendos.values(), ignore_index=True) if todos_dados_dividendos else pd.DataFrame()
                    st.dataframe(df_dividendos_agrupado)
                else:
                    st.info("Nenhum dado de dividendo encontrado na B3 para os tickers/período/tipo de ação especificados.")

            # 3. Bonificações
            if "Bonificações (B3)" in tipos_dados_selecionados:
                st.subheader("3. Bonificações (B3)")
                bonificacoes_encontradas_algum_ticker = False
                for ticker in tickers_list:
                     # st.write(f"Processando bonificações para: {ticker}") # Feedback
                     df_bonificacoes = buscar_bonificacoes_b3(ticker, df_empresas, data_inicio_dt, data_fim_dt)
                     if not df_bonificacoes.empty:
                         todos_dados_bonificacoes[ticker] = df_bonificacoes
                         bonificacoes_encontradas_algum_ticker = True
                         # st.write(f"**{ticker}**") # Não mostra mais individualmente aqui
                         # st.dataframe(df_bonificacoes) # Não mostra mais individualmente aqui

                if bonificacoes_encontradas_algum_ticker:
                     st.write("Bonificações encontradas:")
                     df_bonificacoes_agrupado = pd.concat(todos_dados_bonificacoes.values(), ignore_index=True) if todos_dados_bonificacoes else pd.DataFrame()
                     st.dataframe(df_bonificacoes_agrupado)
                else:
                    st.info("Nenhuma bonificação encontrada na B3 para os tickers/período especificados.")

        # --- Exibir Erros Gerais ---
        # if erros_gerais:
        #    st.subheader("⚠️ Avisos e Erros")
        #    for erro in erros_gerais:
        #        st.warning(erro)

        # --- Geração e Download do Excel ---
        if todos_dados_acoes or todos_dados_dividendos or todos_dados_bonificacoes:
            st.subheader("📥 Download dos Dados")
            formato_excel = st.radio(
                "Escolha o formato do Excel para download:",
                ("Agrupar por tipo de dado", "Uma aba por ticker/tipo de dado"),
                key="excel_format"
            )

            nome_arquivo = f"dados_mercado_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            try:
                with pd.ExcelWriter(nome_arquivo) as writer:
                    if formato_excel == "Agrupar por tipo de dado":
                        if todos_dados_acoes:
                            df_acoes_empilhado = pd.concat(todos_dados_acoes.values(), ignore_index=True)
                            df_acoes_empilhado.to_excel(writer, sheet_name="Precos_Historicos", index=False)
                        if todos_dados_dividendos:
                            df_dividendos_empilhado = pd.concat(todos_dados_dividendos.values(), ignore_index=True)
                            df_dividendos_empilhado.to_excel(writer, sheet_name="Dividendos", index=False)
                        if todos_dados_bonificacoes:
                            df_bonificacoes_empilhado = pd.concat(todos_dados_bonificacoes.values(), ignore_index=True)
                            df_bonificacoes_empilhado.to_excel(writer, sheet_name="Bonificacoes", index=False)

                    else: # Uma aba por ticker/tipo de dado
                        if todos_dados_acoes:
                            for ticker, df_acao in todos_dados_acoes.items():
                                sheet_name = f"Precos_{ticker[:25]}" # Limita tamanho do nome da aba
                                df_acao.to_excel(writer, sheet_name=sheet_name, index=False)
                        if todos_dados_dividendos:
                            for ticker, df_divid in todos_dados_dividendos.items():
                                sheet_name = f"Div_{ticker[:25]}"
                                df_divid.to_excel(writer, sheet_name=sheet_name, index=False)
                        if todos_dados_bonificacoes:
                            for ticker, df_bonif in todos_dados_bonificacoes.items():
                                sheet_name = f"Bonif_{ticker[:25]}"
                                df_bonif.to_excel(writer, sheet_name=sheet_name, index=False)

                # Botão de download
                with open(nome_arquivo, 'rb') as file:
                    st.download_button(
                        label="Baixar arquivo Excel",
                        data=file,
                        file_name=nome_arquivo,
                        mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                    )
            except Exception as e:
                 st.error(f"Erro ao gerar o arquivo Excel: {e}")

        elif not erros_gerais:
             st.info("Nenhum dado encontrado para os critérios selecionados.")

    else:
        st.warning("Por favor, preencha todos os campos: tickers, datas e selecione ao menos um tipo de dado.")

# --- Rodapé ---
st.markdown("""
---
**Fontes dos dados:**
- Preços Históricos: [Yahoo Finance](https://finance.yahoo.com)
- Dividendos e Bonificações: [API B3](https://www.b3.com.br) (via endpoints não oficiais)
- Mapeamento Ticker/Empresa: Arquivo Excel mantido externamente.
- Código fonte base: [Github tovarich86](https://github.com/tovarich86/ticker) (modificado)
""")
