import streamlit as st
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

# Função para buscar dados das ações
def buscar_dados_acoes(tickers_input, data_inicio_input, data_fim_input):
    # Convertendo as datas para o formato esperado pelo yfinance
    data_inicio = datetime.strptime(data_inicio_input, "%d/%m/%Y").strftime("%Y-%m-%d")
    data_fim = datetime.strptime(data_fim_input, "%d/%m/%Y").strftime("%Y-%m-%d")

    # Ajustando a data final para incluir o dia especificado
    data_fim_ajustada = (datetime.strptime(data_fim, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")

    # Processando os tickers
    tickers = [ticker.strip() + '.SA' if not ticker.endswith('.SA') and ticker != '^BVSP' else ticker.strip() for ticker in tickers_input.split(",")]

    dados_finais = pd.DataFrame()  # DataFrame final para acumular os resultados

    for ticker in tickers:
        try:
            # Baixando os dados de um único ticker
            dados = yf.download(ticker, start=data_inicio, end=data_fim_ajustada)
            if not dados.empty:
                dados['Ticker'] = ticker  # Adicionando uma coluna para o ticker
                dados.reset_index(inplace=True)  # Transformando o índice de datas em uma coluna
                dados['Date'] = dados['Date'].dt.strftime('%d/%m/%Y')  # Ajustando o formato da data

                # Adicionando ao DataFrame final
                dados_finais = pd.concat([dados_finais, dados])
        except Exception as e:
            st.error(f"Erro ao buscar dados para o ticker {ticker}: {e}")
            continue

    if not dados_finais.empty:
        # Reordenando as colunas
        cols = ['Ticker'] + [col for col in dados_finais.columns if col != 'Ticker']
        dados_finais = dados_finais[cols]

    return dados_finais

# Interface do Streamlit
st.title('Consulta de Dados de Ações')

# Entrada do usuário
tickers_input = st.text_input("Digite os tickers separados por vírgula (ex: PETR4, VALE3, ^BVSP):")
data_inicio_input = st.text_input("Digite a data de início (dd/mm/aaaa):")
data_fim_input = st.text_input("Digite a data de fim (dd/mm/aaaa):")

# Botão para buscar dados
if st.button('Buscar Dados'):
    if tickers_input and data_inicio_input and data_fim_input:
        dados = buscar_dados_acoes(tickers_input, data_inicio_input, data_fim_input)
        if not dados.empty:
            st.write("### Dados de Ações:")
            st.dataframe(dados)
            
            # Opção para download do Excel
            nome_arquivo = f"dados_acoes_{tickers_input.replace(',', '_').replace(' ', '')}.xlsx"
            dados.to_excel(nome_arquivo, index=False)
            with open(nome_arquivo, 'rb') as file:
                st.download_button(label="Baixar arquivo Excel", data=file, file_name=nome_arquivo)
        else:
            st.warning("Nenhum dado encontrado para os tickers e período especificados.")
    else:
        st.error("Por favor, preencha todos os campos.")
