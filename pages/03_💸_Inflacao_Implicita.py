# Arquivo: pages/03_💸_Inflacao_Implicita.py
import streamlit as st
import pandas as pd
from io import BytesIO
import sys
import os

# Adiciona o diretório raiz ao path para importar o src
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.treasury_service import carregar_dados_tesouro, calcular_inflacao_implicita

st.set_page_config(page_title="Inflação Implícita", layout="wide")

st.title("📊 Cálculo da Inflação Implícita - Tesouro Direto")

st.markdown("""
A **Inflação Implícita** é a expectativa de inflação do mercado, calculada pela diferença entre as taxas dos títulos **Prefixados** e **IPCA+**.
""")

# Fórmula (LaTeX)
st.latex(r"""
\text{Inflação Implícita} = \left( \frac{1 + \text{Taxa Prefixada}}{1 + \text{Taxa IPCA}} \right) - 1
""")

with st.spinner("Baixando dados atualizados do Tesouro Direto..."):
    df_raw = carregar_dados_tesouro()

if df_raw.empty:
    st.error("Falha ao carregar dados do Tesouro. Tente novamente mais tarde.")
    st.stop()

# Filtros de Data
min_date = df_raw["Data Base"].min()
max_date = df_raw["Data Base"].max()

col1, col2 = st.columns(2)
with col1:
    data_base_input = st.date_input("📅 Data Base (Referência):", value=max_date, min_value=min_date, max_value=max_date)

# Processamento
data_base_ts = pd.to_datetime(data_base_input)
df_resultado, erro = calcular_inflacao_implicita(df_raw, data_base_ts)

if erro:
    st.warning(f"⚠️ {erro}")
else:
    # Formatação para exibição
    df_show = df_resultado.copy()
    cols_data = ["Data Base", "Vencimento Prefixado", "Vencimento IPCA+ Ref"]
    for col in cols_data:
        df_show[col] = df_show[col].dt.strftime("%d/%m/%Y")
    
    df_show["Inflação Implícita (%)"] = df_show["Inflação Implícita (%)"].map("{:.2f}%".format)
    df_show["Taxa Prefixada"] = df_show["Taxa Prefixada"].map("{:.2f}%".format)
    df_show["Taxa IPCA+"] = df_show["Taxa IPCA+"].map("{:.2f}%".format)

    st.subheader(f"Resultados para {data_base_input.strftime('%d/%m/%Y')}")
    st.dataframe(df_show, use_container_width=True)

    # Botão de Download Excel
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df_resultado.to_excel(writer, index=False, sheet_name="Inflacao_Implicita")
    
    st.download_button(
        label="📥 Baixar Resultado em Excel",
        data=output.getvalue(),
        file_name=f"inflacao_implicita_{data_base_input}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
