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
        # Ignora tickers que não parecem ser brasileiros (sem números) para esta busca B3
        return pd.DataFrame()

    ticker_info = get_ticker_info(ticker, empresas_df)

    if not ticker_info:
        st.warning(f"Info não encontrada para {ticker} na planilha. Dividendos B3 não buscados.")
        return pd.DataFrame()

    trading_name = ticker_info.get('trading_name')
    desired_type_stock = ticker_info.get('type_stock') # Tipo de ação (ON, PN, UNT) do ticker buscado

    if not trading_name:
         st.warning(f"Nome pregão não encontrado para {ticker}. Dividendos B3 não buscados.")
         return pd.DataFrame()
    if not desired_type_stock:
        st.warning(f"typeStock não encontrado para {ticker}. Não é possível filtrar dividendos B3.")
        return pd.DataFrame()

    all_dividends = []
    current_page = 1
    total_pages = 1 # Inicializa com 1 para fazer a primeira requisição
    api_called = False # Flag para saber se a API foi chamada

    # Loop de paginação
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
                 if current_page == 1:
                     # st.info(f"Resposta vazia da API de dividendos para {ticker} ({trading_name}) na página 1.")
                     pass # Silencioso se a primeira página estiver vazia
                 break # Interrompe se uma página subsequente vier vazia

            try:
                response_json = response.json()
            except json.JSONDecodeError:
                st.error(f"Erro JSON dividendos B3 para {ticker} (pág {current_page}).")
                break

            # Atualiza o total de páginas na primeira requisição bem-sucedida
            if current_page == 1 and 'page' in response_json and 'totalPages' in response_json['page']:
                total_pages = int(response_json['page']['totalPages'])

            if 'results' in response_json and response_json['results']:
                all_dividends.extend(response_json['results'])
            elif current_page == 1:
                 # Sai se não houver resultados na primeira página
                 break

            # Pausa leve
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
        # if api_called: st.info(f"Nenhum dividendo encontrado na B3 para {ticker} ({trading_name}).")
        return pd.DataFrame()

    # Cria DataFrame e inicia filtros
    df = pd.DataFrame(all_dividends)

    # 1. Filtrar pelo typeStock desejado
    if 'typeStock' in df.columns:
         df['typeStock'] = df['typeStock'].astype(str).str.strip().str.upper()
         df_filtered_type = df[df['typeStock'] == desired_type_stock].copy()
         if df_filtered_type.empty:
              # st.info(f"Dividendos B3 para {trading_name} encontrados, mas nenhum do tipo {desired_type_stock} ({ticker}).")
              return pd.DataFrame()
         df = df_filtered_type
    else:
         st.warning(f"Coluna 'typeStock' não encontrada nos dividendos B3 para {ticker}. Não foi possível filtrar.")
         # Continua sem filtro de tipo

    # 2. Adicionar coluna Ticker
    df['Ticker'] = ticker

    # 3. Converter datas e filtrar pelo período
    if 'lastDatePriorEx' in df.columns:
        df['lastDatePriorEx_dt'] = pd.to_datetime(df['lastDatePriorEx'], format='%d/%m/%Y', errors='coerce')
        df = df.dropna(subset=['lastDatePriorEx_dt'])
        df = df[(df['lastDatePriorEx_dt'] >= data_inicio) & (df['lastDatePriorEx_dt'] <= data_fim)]
        df = df.drop(columns=['lastDatePriorEx_dt'])
    else:
        st.warning(f"Coluna 'lastDatePriorEx' não encontrada dividendos B3 {ticker}. Não foi possível filtrar data.")
        return pd.DataFrame()

    if df.empty:
        # st.info(f"Nenhum dividendo B3 para {ticker} ({desired_type_stock}) no período selecionado.")
        return pd.DataFrame()

    # Reordenar colunas
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
             # st.info(f"Resposta vazia API eventos B3 para {ticker} (CODE: {code}).")
             return pd.DataFrame()
        try:
            data = response.json()
        except json.JSONDecodeError:
             # st.info(f"Resposta inválida (não JSON) API eventos B3 para {ticker} (CODE: {code}).")
             return pd.DataFrame()

        # Foca em 'stockDividends' que representa bonificações neste endpoint
        if not isinstance(data, list) or not data or "stockDividends" not in data[0] or not data[0]["stockDividends"]:
            # st.info(f"Nenhum dado de bonificação ('stockDividends') encontrado B3 para {ticker} (CODE: {code}).")
            return pd.DataFrame()

        df = pd.DataFrame(data[0]["stockDividends"])
        if df.empty:
            return pd.DataFrame()

        # Adiciona Ticker e filtra por data
        df['Ticker'] = ticker
        if 'lastDatePrior' in df.columns:
             df['lastDatePrior_dt'] = pd.to_datetime(df['lastDatePrior'], format='%d/%m/%Y', errors='coerce')
             df = df.dropna(subset=['lastDatePrior_dt'])
             df = df[(df['lastDatePrior_dt'] >= data_inicio) & (df['lastDatePrior_dt'] <= data_fim)]
             df = df.drop(columns=['lastDatePrior_dt'])
        else:
             st.warning(f"Coluna 'lastDatePrior' não encontrada eventos B3 {ticker}. Não foi possível filtrar data.")
             return pd.DataFrame()

        if df.empty:
            # st.info(f"Nenhum evento B3 (bonificação) para {ticker} no período selecionado.")
            return pd.DataFrame()

        # Reordena colunas
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
        # Converte datas de string para datetime objects primeiro
        data_inicio_dt = datetime.strptime(data_inicio_input, "%d/%m/%Y")
        data_fim_dt = datetime.strptime(data_fim_input, "%d/%m/%Y")

        # Formata para string YYYY-MM-DD para API yfinance
        data_inicio_str = data_inicio_dt.strftime("%Y-%m-%d")
        # Ajusta data fim para incluir o dia na busca do yfinance
        data_fim_ajustada_str = (data_fim_dt + timedelta(days=1)).strftime("%Y-%m-%d")
    except ValueError:
        st.error("Formato de data inválido para preços. Use dd/mm/aaaa.")
        return {}, ["Formato de data inválido."]

    tickers_list = [ticker.strip().upper() for ticker in tickers_input.split(',') if ticker.strip()]
    dados_acoes_dict = {}
    erros = []

    for ticker in tickers_list:
        ticker_yf = ticker # Ticker base
        # Adiciona '.SA' para tickers brasileiros
        if any(char.isdigit() for char in ticker) and not ticker.endswith('.SA'):
             ticker_yf = ticker + '.SA'

        try:
            # Feedback removido daqui para não poluir a barra de progresso
            # st.write(f"Buscando preços históricos para {ticker} ({ticker_yf})...")

            # CORREÇÃO: Adicionado multi_level_index=False
            dados = yf.download(ticker_yf, start=data_inicio_str, end=data_fim_ajustada_str,
                                auto_adjust=False, progress=False,
                                multi_level_index=False) # <<< ESSENCIAL PARA EVITAR KeyError

            if not dados.empty:
                dados.reset_index(inplace=True)

                # Garante que 'Date' é datetime antes de filtrar
                dados['Date'] = pd.to_datetime(dados['Date'])
                # Filtra EXATAMENTE pelo período solicitado pelo usuário (usa datetime objects)
                dados = dados[(dados['Date'] >= data_inicio_dt) & (dados['Date'] <= data_fim_dt)]

                if dados.empty: # Verifica se sobrou algo após o filtro de data
                    # Não adiciona erro se apenas não houver dados no período exato
                    # erros.append(f"Sem dados de preços para {ticker} ({ticker_yf}) no período exato.")
                    continue # Pula para o próximo ticker

                # Formatar Data para dd/mm/aaaa *APÓS* filtrar
                dados['Date'] = dados['Date'].dt.strftime('%d/%m/%Y')

                # Adicionar coluna Ticker (original, sem .SA)
                dados['Ticker'] = ticker
                # Reordenar para Ticker ser a primeira coluna
                cols = ['Ticker', 'Date'] + [col for col in dados.columns if col not in ['Ticker', 'Date']]
                dados = dados[cols]

                dados_acoes_dict[ticker] = dados
            else:
                # Adiciona erro se yfinance não retornou NADA
                 erros.append(f"Sem dados de preços (yfinance) encontrados para {ticker} ({ticker_yf}).")

        except Exception as e:
            erros.append(f"Erro ao buscar preços (yfinance) para {ticker} ({ticker_yf}): {e}")
            # Opcional: Mostrar traceback para depuração
            # st.error(f"Traceback para {ticker}: {traceback.format_exc()}")
            continue

    return dados_acoes_dict, erros

# ============================================
# Interface do Streamlit
# ============================================
st.set_page_config(layout="wide")
st.title('Consulta Dados de Mercado B3 e Yahoo Finance')

# --- Carrega o DataFrame de empresas ---
df_empresas = carregar_empresas()

if df_empresas.empty:
    st.error("Falha ao carregar lista de empresas. Verifique URL/arquivo. Aplicação interrompida.")
    st.stop()

# --- Entradas do Usuário ---
col1, col2 = st.columns(2)
with col1:
    tickers_input = st.text_input("Tickers (separados por vírgula):", key="tickers", placeholder="Ex: PETR4, VALE3, MGLU3")
with col2:
    # Nomes das opções como definido anteriormente
    tipos_dados_selecionados = st.multiselect(
        "Selecione os dados:",
        ["Preços(YFinance)", "Dividendos (B3)", "Eventos societários (B3)"],
        default=["Preços(YFinance)"], # Garante que o default está na lista
        key="data_types"
    )

col3, col4 = st.columns(2)
# Usar placeholder para exemplos de data
today_str = datetime.now().strftime("%d/%m/%Y")
last_year_str = (datetime.now() - timedelta(days=365)).strftime("%d/%m/%Y")
with col3:
    data_inicio_input = st.text_input("Data de início (dd/mm/aaaa):", key="date_start", value=last_year_str)
with col4:
    data_fim_input = st.text_input("Data de fim (dd/mm/aaaa):", key="date_end", value=today_str)


# --- Botão e Lógica Principal ---
if st.button('Buscar Dados', key="search_button"):
    # Validações iniciais
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

    # Dicionários para resultados
    todos_dados_precos = {}
    todos_dados_dividendos = {}
    todos_dados_eventos = {}
    erros_gerais = []

    # --- Barra de Progresso e Status ---
    progress_bar = st.progress(0)
    status_text = st.empty()
    # Cálculo preciso do total de passos
    total_steps = 0
    if "Preços(YFinance)" in tipos_dados_selecionados:
        total_steps += len(tickers_list) # 1 passo por ticker para yfinance
    if "Dividendos (B3)" in tipos_dados_selecionados:
        total_steps += len(tickers_list) # 1 passo por ticker para dividendos
    if "Eventos societários (B3)" in tipos_dados_selecionados:
        total_steps += len(tickers_list) # 1 passo por ticker para eventos
    current_step = 0

    def update_progress(steps_done=1):
        nonlocal current_step
        current_step += steps_done
        if total_steps > 0:
            progress_bar.progress(min(current_step / total_steps, 1.0))

    # --- Busca de Dados ---
    with st.spinner('Buscando dados...'):
        # 1. Preços Históricos
        if "Preços(YFinance)" in tipos_dados_selecionados:
            status_text.text(f"Buscando Preços Históricos (Yahoo Finance)...")
            dados_acoes_dict, erros_acoes = buscar_dados_acoes(tickers_input, data_inicio_input, data_fim_input)
            if dados_acoes_dict:
                todos_dados_precos = dados_acoes_dict
            if erros_acoes:
                erros_gerais.extend(erros_acoes)
            update_progress(len(tickers_list)) # Atualiza progresso para todos tickers de uma vez


        # 2. Dividendos B3
        if "Dividendos (B3)" in tipos_dados_selecionados:
            dividendos_encontrados_algum_ticker = False
            for i, ticker in enumerate(tickers_list):
                 status_text.text(f"Buscando Dividendos (B3) para {ticker} ({i+1}/{len(tickers_list)})...")
                 df_dividendos = buscar_dividendos_b3(ticker, df_empresas, data_inicio_dt, data_fim_dt)
                 if not df_dividendos.empty:
                     todos_dados_dividendos[ticker] = df_dividendos
                     dividendos_encontrados_algum_ticker = True
                 update_progress() # Atualiza 1 passo por ticker


        # 3. Eventos Societários B3
        if "Eventos societários (B3)" in tipos_dados_selecionados:
            eventos_encontrados_algum_ticker = False
            for i, ticker in enumerate(tickers_list):
                 status_text.text(f"Buscando Eventos Societários (B3) para {ticker} ({i+1}/{len(tickers_list)})...")
                 # Usando o nome correto da função
                 df_eventos = buscar_eventos_societarios_b3(ticker, df_empresas, data_inicio_dt, data_fim_dt)
                 if not df_eventos.empty:
                     todos_dados_eventos[ticker] = df_eventos
                     eventos_encontrados_algum_ticker = True
                 update_progress() # Atualiza 1 passo por ticker

    status_text.text("Busca concluída!")
    progress_bar.empty()


    # --- Exibição dos Resultados Agrupados ---
    st.markdown("---") # Separador visual
    dados_exibidos = False # Flag para saber se algo foi exibido

    if "Preços(YFinance)" in tipos_dados_selecionados:
        st.subheader("1. Preços Históricos (Yahoo Finance)")
        if todos_dados_precos:
             df_precos_agrupado = pd.concat(todos_dados_precos.values(), ignore_index=True)
             st.dataframe(df_precos_agrupado)
             dados_exibidos = True
        elif not any("preços (yfinance)" in e.lower() for e in erros_gerais): # Mostra info se não achou e não teve erro *específico* de preço
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


    # --- Exibir Erros Gerais ---
    if erros_gerais:
       st.subheader("⚠️ Avisos e Erros")
       for erro in erros_gerais:
           st.warning(erro)

    # --- Geração e Download do Excel ---
    if dados_exibidos: # Só mostra opção de download se houve dados
        st.subheader("📥 Download dos Dados")
        formato_excel = st.radio(
            "Escolha o formato do Excel:",
            ("Agrupar por tipo de dado", "Uma aba por ticker/tipo"),
            key="excel_format"
        )

        nome_arquivo = f"dados_mercado_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        try:
            # Usar BytesIO para criar o Excel em memória
            from io import BytesIO
            output = BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                if formato_excel == "Agrupar por tipo de dado":
                    if todos_dados_precos:
                        df_precos_empilhado = pd.concat(todos_dados_precos.values(), ignore_index=True)
                        df_precos_empilhado.to_excel(writer, sheet_name="Precos_YFinance", index=False)
                    if todos_dados_dividendos:
                        df_dividendos_empilhado = pd.concat(todos_dados_dividendos.values(), ignore_index=True)
                        df_dividendos_empilhado.to_excel(writer, sheet_name="Dividendos", index=False)
                    if todos_dados_eventos:
                        df_eventos_empilhado = pd.concat(todos_dados_eventos.values(), ignore_index=True)
                        df_eventos_empilhado.to_excel(writer, sheet_name="Eventos_Societarios", index=False)

                else: # Uma aba por ticker/tipo
                    if todos_dados_precos:
                        for ticker, df_acao in todos_dados_precos.items():
                            # Limita nome da aba e remove caracteres inválidos
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
            # O writer é fechado automaticamente ao sair do 'with'

            # Botão de download usando os bytes
            st.download_button(
                label="Baixar arquivo Excel",
                data=output.getvalue(), # Pega os bytes do BytesIO
                file_name=nome_arquivo,
                mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
        except Exception as e:
             st.error(f"Erro ao gerar o arquivo Excel: {e}")
             st.error(traceback.format_exc()) # Mostra mais detalhes do erro

    elif not erros_gerais: # Se não exibiu dados E não teve erros
         st.info("Nenhum dado encontrado para os critérios selecionados.")

# --- Rodapé ---
st.markdown("""
---
**Fontes:** Yahoo Finance (Preços), API B3 (Dividendos, Eventos). Mapeamento via Excel externo.
Código base por [tovarich86](https://github.com/tovarich86/ticker), modificado.
""")
