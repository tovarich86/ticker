import base64
import json
import requests

# Função para buscar nome do pregão na planilha
def get_trading_name(ticker, df_empresas):
    empresa = df_empresas[df_empresas['Tickers'].astype(str).str.contains(ticker, na=False, regex=False)]
    if not empresa.empty:
        return empresa.iloc[0]['Nome do Pregão']
    raise ValueError(f'Ticker {ticker} não encontrado.')

# Função para buscar dividendos usando a API da B3
def buscar_dividendos_b3(ticker, df_empresas):
    try:
        trading_name = get_trading_name(ticker, df_empresas)

        # Criar payload JSON para codificação Base64
        payload = {
            "language": "pt-br",
            "pageNumber": 1,
            "pageSize": 20,
            "tradingName": trading_name
        }
        payload_encoded = base64.b64encode(json.dumps(payload).encode()).decode()

        # URL formatada corretamente com o payload em Base64
        url = f"https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetListedCashDividends/{payload_encoded}"
        response = requests.get(url)

        if response.status_code != 200:
            raise ValueError(f"Erro na API B3: {response.status_code}")

        response_json = response.json()
        if 'results' not in response_json or not response_json['results']:
            raise ValueError(f'Dividendos não encontrados para {trading_name} ({ticker}).')

        df = pd.DataFrame(response_json['results'])
        df['Ticker'] = ticker  # Adiciona o Ticker como uma nova coluna
        return df

    except Exception as e:
        print(f"Dividendos não encontrados para o ticker {ticker}: {e}")
        return pd.DataFrame()
