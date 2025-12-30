import requests
import pandas as pd
import streamlit as st
from io import StringIO
from scipy.spatial import cKDTree

# URL oficial
CSV_TESOURO_URL = "https://www.tesourotransparente.gov.br/ckan/dataset/df56aa42-484a-4a59-8184-7676580c81e3/resource/796d2059-14e9-44e3-80c9-2d9e30b405c1/download/PrecoTaxaTesouroDireto.csv"

@st.cache_data(ttl=3600, show_spinner=False)
def carregar_dados_tesouro(arquivo_manual=None):
    """
    Baixa dados do Tesouro (Lógica Clássica Pandas + Requests).
    """
    try:
        if arquivo_manual:
            return pd.read_csv(
                arquivo_manual, 
                sep=';', 
                decimal=',', 
                parse_dates=['Data Base', 'Data Vencimento'], 
                dayfirst=True
            )
        else:
            # Configuração para evitar bloqueio do site .gov.br
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            }
            # verify=False é necessário pois o certificado do Tesouro as vezes falha
            response = requests.get(CSV_TESOURO_URL, headers=headers, verify=False, timeout=45)
            response.raise_for_status()
            
            # Leitura direta com Pandas (StringIO simula um arquivo na memória)
            return pd.read_csv(
                StringIO(response.text), 
                sep=';', 
                decimal=',', 
                parse_dates=['Data Base', 'Data Vencimento'], 
                dayfirst=True
            )

    except Exception as e:
        print(f"Erro no download do Tesouro: {e}")
        return pd.DataFrame()

def calcular_inflacao_implicita(df_raw, data_base_ref):
    """
    Calcula a inflação implícita cruzando títulos Prefixados e IPCA+.
    """
    if df_raw.empty: return pd.DataFrame(), "DataFrame vazio recebido."

    # 1. Filtra pela Data Base
    df_dia = df_raw[df_raw["Data Base"] == data_base_ref].copy()
    if df_dia.empty: return pd.DataFrame(), "Sem dados para esta data."

    # 2. Separa Títulos
    mask_pre = df_dia["Tipo Titulo"].str.contains("Prefixado", case=False, na=False) & \
               ~df_dia["Tipo Titulo"].str.contains("Juros Semestrais", case=False, na=False)
    mask_ipca = df_dia["Tipo Titulo"].str.contains("Tesouro IPCA\\+$", regex=True, case=False, na=False)

    df_pre = df_dia[mask_pre].copy()
    df_ipca = df_dia[mask_ipca].copy()

    if df_pre.empty or df_ipca.empty:
        return pd.DataFrame(), "Faltam dados (Prefixado ou IPCA+) para calcular."

    # 3. Interpolação (cKDTree)
    # Converte datas para números inteiros para cálculo de distância
    df_ipca["Vencimento_Num"] = df_ipca["Data Vencimento"].dt.strftime("%Y%m%d").astype(int)
    df_pre["Vencimento_Num"] = df_pre["Data Vencimento"].dt.strftime("%Y%m%d").astype(int)

    if len(df_ipca) < 2:
         return pd.DataFrame(), "Poucos dados IPCA+ para interpolação."

    df_ipca_sorted = df_ipca.sort_values("Vencimento_Num")
    vencimentos_ipca = df_ipca_sorted["Vencimento_Num"].values.reshape(-1, 1)
    
    # Cria árvore de busca rápida
    tree = cKDTree(vencimentos_ipca)

    def _match_ipca(vencimento_num):
        _, idx = tree.query([[vencimento_num]])
        row = df_ipca_sorted.iloc[idx[0]]
        return row["Data Vencimento"], row["Taxa Compra Manha"]

    # 4. Cálculo final
    resultados = []
    for idx, row in df_pre.iterrows():
        venc_match, taxa_ipca = _match_ipca(row["Vencimento_Num"])
        
        # Fórmula de Fisher: (1 + Pre) / (1 + Real) - 1
        inflacao = ((1 + row["Taxa Compra Manha"] / 100) / (1 + taxa_ipca / 100) - 1) * 100
        
        resultados.append({
            "Data Base": row["Data Base"],
            "Vencimento Prefixado": row["Data Vencimento"],
            "Taxa Prefixada": row["Taxa Compra Manha"],
            "Vencimento IPCA+ Ref": venc_match,
            "Taxa IPCA+": taxa_ipca,
            "Inflação Implícita (%)": inflacao
        })

    return pd.DataFrame(resultados), None
