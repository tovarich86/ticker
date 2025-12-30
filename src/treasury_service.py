# Arquivo: src/treasury_service.py
import pandas as pd
import polars as pl
import numpy as np
import streamlit as st
from scipy.spatial import cKDTree

CSV_TESOURO_URL = "https://www.tesourotransparente.gov.br/ckan/dataset/df56aa42-484a-4a59-8184-7676580c81e3/resource/796d2059-14e9-44e3-80c9-2d9e30b405c1/download/PrecoTaxaTesouroDireto.csv"

@st.cache_data(ttl=3600, show_spinner=False)
def carregar_dados_tesouro():
    """
    Baixa dados do Tesouro usando Polars para alta performance e cacheia o resultado.
    """
    try:
        # Polars lê diretamente da URL muito mais rápido que requests+pandas
        df_pl = pl.read_csv(
            CSV_TESOURO_URL, 
            separator=';', 
            decimal=',', 
            ignore_errors=True,
            try_parse_dates=True
        )
        
        # Convertemos para Pandas aqui para manter compatibilidade com a lógica matemática existente
        # e garantir os tipos de data corretos
        df = df_pl.to_pandas()
        
        # Garante a conversão correta das datas (Polars às vezes deixa como string se o formato variar)
        df['Data Base'] = pd.to_datetime(df['Data Base'], dayfirst=True, errors='coerce')
        df['Data Vencimento'] = pd.to_datetime(df['Data Vencimento'], dayfirst=True, errors='coerce')
        
        return df
    except Exception as e:
        print(f"Erro Tesouro (Polars): {e}")
        return pd.DataFrame()

def calcular_inflacao_implicita(df_raw, data_base_ref):
    """
    Lógica matemática de interpolação (Mantida em Pandas/Scipy pois opera em memória pequena filtrada).
    """
    # 1. Filtra pela Data Base selecionada
    df_dia = df_raw[df_raw["Data Base"] == data_base_ref].copy()
    if df_dia.empty: return pd.DataFrame(), "Sem dados para esta data."

    # 2. Separa Prefixados vs IPCA+
    mask_pre = df_dia["Tipo Titulo"].str.contains("Prefixado", case=False, na=False) & \
               ~df_dia["Tipo Titulo"].str.contains("Juros Semestrais", case=False, na=False)
    
    mask_ipca = df_dia["Tipo Titulo"].str.contains("Tesouro IPCA\\+$", regex=True, case=False, na=False)

    df_pre = df_dia[mask_pre].copy()
    df_ipca = df_dia[mask_ipca].copy()

    if df_pre.empty or df_ipca.empty:
        return pd.DataFrame(), "Faltam dados de Prefixado ou IPCA+ para cálculo."

    # 3. Prepara interpolação
    df_ipca["Vencimento_Num"] = df_ipca["Data Vencimento"].dt.strftime("%Y%m%d").astype(int)
    df_pre["Vencimento_Num"] = df_pre["Data Vencimento"].dt.strftime("%Y%m%d").astype(int)

    # Verifica se há dados suficientes para interpolação
    if len(df_ipca) < 2:
         return pd.DataFrame(), "Poucos vértices IPCA+ para interpolação."

    df_ipca_sorted = df_ipca.sort_values("Vencimento_Num")
    vencimentos_ipca = df_ipca_sorted["Vencimento_Num"].values.reshape(-1, 1)
    
    tree = cKDTree(vencimentos_ipca)

    def _match_ipca(vencimento_num):
        _, idx = tree.query([[vencimento_num]])
        row = df_ipca_sorted.iloc[idx[0]]
        return row["Data Vencimento"], row["Taxa Compra Manha"]

    # 4. Aplica o cruzamento
    resultados = []
    for idx, row in df_pre.iterrows():
        venc_match, taxa_ipca = _match_ipca(row["Vencimento_Num"])
        
        # Fórmula de Fisher
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
