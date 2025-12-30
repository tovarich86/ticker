# Arquivo: src/di_service.py
import requests
import pandas as pd
import base64
import json
from io import BytesIO

def gerar_url_b3_base64(data):
    """Gera a URL codificada necessária para a API da B3."""
    data_iso = data.strftime("%Y-%m-%d")
    params = {
        "language": "pt-br",
        "date": data_iso,
        "id": "PRE"
    }
    json_string = json.dumps(params, separators=(',', ':'))
    json_base64 = base64.b64encode(json_string.encode()).decode()
    return f"https://sistemaswebb3-derivativos.b3.com.br/referenceRatesProxy/Search/GetDownloadFile/{json_base64}"

def consultar_taxas_di(data):
    """
    Retorna um DataFrame com as taxas DI para a data informada.
    Retorna (DataFrame, str_erro). Se sucesso, str_erro é None.
    """
    url = gerar_url_b3_base64(data)
    
    # Session para performance (keep-alive)
    with requests.Session() as session:
        try:
            response = session.get(url, timeout=15)
            response.raise_for_status()
            
            conteudo_base64 = response.text.strip()
            # Limpa aspas extras se a API retornar string JSON
            if conteudo_base64.startswith('"') and conteudo_base64.endswith('"'):
                conteudo_base64 = conteudo_base64[1:-1]
            
            # Decodifica o arquivo CSV que veio em Base64
            csv_bytes = base64.b64decode(conteudo_base64)
            
            df = pd.read_csv(
                BytesIO(csv_bytes), 
                sep=';', 
                encoding='latin1', 
                decimal=',',
                engine='python'
            )
            
            if df.empty:
                return None, "Arquivo retornado pela B3 está vazio."
            
            # Padronização de colunas
            df.columns = [col.strip() for col in df.columns]
            
            # Renomeia para facilitar uso no frontend
            mapa = {
                'Descrição da Taxa': 'DESCRICAO',
                'Dias Úteis': 'DIAS_UTEIS',
                'Dias Corridos': 'DIAS_CORRIDOS',
                'Preço/Taxa': 'TAXA'
            }
            df = df.rename(columns=mapa)
            
            return df, None

        except Exception as e:
            return None, str(e)
