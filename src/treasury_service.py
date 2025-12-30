# Arquivo: src/treasury_service.py
import requests
import pandas as pd
import numpy as np
from io import StringIO
from scipy.spatial import cKDTree

CSV_TESOURO_URL = "https://www.tesourotransparente.gov.br/ckan/dataset/df56aa42-484a-4a59-8184-7676580c81e3/resource/796d2059-14e9-44e3-80c9-2d9e30b405c1/download/PrecoTaxaTesouroDireto.csv"

def carregar_dados_tesouro():
    """Baixa e faz parse inicial do CSV do Tesouro Direto."""
    try:
        response = requests.get(CSV_TESOURO_URL, timeout=30)
        response.raise_for_status()
        
        # O Tesouro usa CSV com ; e vírgula decimal
        df = pd.read_csv(
            StringIO(response.text), 
            sep=';', 
            decimal=',', 
            parse_dates=['Data Base', 'Data Vencimento'], 
            dayfirst=True
        )
        return df
    except Exception as e:
        print(f"Erro Tesouro: {e}")
        return pd.DataFrame()

def calcular_inflacao_implicita(df_raw, data_base_ref):
    """
    Filtra os dados para uma data específica e cruza títulos Prefixados com IPCA+
    para encontrar a inflação implícita.
    """
    # 1. Filtra pela Data Base selecionada
    df_dia = df_raw[df_raw["Data Base"] == data_base_ref].copy()
    if df_dia.empty: return pd.DataFrame(), "Sem dados para esta data."

    # 2. Separa Prefixados vs IPCA+
    # Remove "Juros Semestrais" para simplificar a curva padrão (opcional, mas comum na metodologia)
    mask_pre = df_dia["Tipo Titulo"].str.contains("Prefixado", case=False, na=False) & \
               ~df_dia["Tipo Titulo"].str.contains("Juros Semestrais", case=False, na=False)
    
    mask_ipca = df_dia["Tipo Titulo"].str.contains("Tesouro IPCA\\+$", regex=True, case=False, na=False)

    df_pre = df_dia[mask_pre].copy()
    df_ipca = df_dia[mask_ipca].copy()

    if df_pre.empty or df_ipca.empty:
        return pd.DataFrame(), "Faltam dados de Prefixado ou IPCA+ para cálculo."

    # 3. Prepara interpolação (busca do vizinho mais próximo por data de vencimento)
    df_ipca["Vencimento_Num"] = df_ipca["Data Vencimento"].dt.strftime("%Y%m%d").astype(int)
    df_pre["Vencimento_Num"] = df_pre["Data Vencimento"].dt.strftime("%Y%m%d").astype(int)

    df_ipca_sorted = df_ipca.sort_values("Vencimento_Num")
    vencimentos_ipca = df_ipca_sorted["Vencimento_Num"].values.reshape(-1, 1)
    
    # Árvore de busca rápida
    tree = cKDTree(vencimentos_ipca)

    def _match_ipca(vencimento_num):
        # Encontra o índice do título IPCA+ com vencimento mais próximo
        _, idx = tree.query([[vencimento_num]])
        row = df_ipca_sorted.iloc[idx[0]]
        return row["Data Vencimento"], row["Taxa Compra Manha"]

    # 4. Aplica o cruzamento
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
