import streamlit as st
import pandas as pd
from io import BytesIO
from datetime import datetime
import sys, os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src import di_service

st.set_page_config(page_title="Taxas DI1", layout="wide")
st.title("📉 Curva de Juros Futuros (DI1)")

st.markdown("Consulta oficial diretamente da B3 (arquivos de Preços Referenciais).")

col_mode = st.radio("Modo de Consulta:", ["Data Única", "Múltiplas Datas (Arquivo)"], horizontal=True)

datas_para_buscar = []

if col_mode == "Data Única":
    d = st.date_input("Selecione a Data", datetime.now())
    datas_para_buscar = [d]
else:
    f = st.file_uploader("Envie Excel/CSV contendo coluna 'Data'", type=['xlsx', 'csv'])
    if f:
        df_in = pd.read_excel(f) if f.name.endswith('.xlsx') else pd.read_csv(f)
        # Tenta achar coluna de data
        col_data = next((c for c in df_in.columns if 'data' in c.lower()), None)
        if col_data:
            datas_para_buscar = pd.to_datetime(df_in[col_data], errors='coerce').dropna().dt.date.unique()
            st.success(f"{len(datas_para_buscar)} datas identificadas.")
        else:
            st.error("Não encontrei coluna com nome 'Data' no arquivo.")

if st.button("Buscar Taxas", type="primary"):
    if not len(datas_para_buscar):
        st.warning("Nenhuma data selecionada.")
        st.stop()

    results = []
    bar = st.progress(0)
    
    for i, data_ref in enumerate(datas_para_buscar):
        df, err = di_service.consultar_taxas_di(data_ref)
        if df is not None:
            df.insert(0, "DATA_REF", data_ref.strftime("%d/%m/%Y"))
            results.append(df)
        else:
            # Opcional: Mostrar erro apenas se for Data Única para não poluir
            if col_mode == "Data Única": st.error(f"{data_ref}: {err}")
            
        bar.progress((i+1)/len(datas_para_buscar))
        
    if results:
        df_final = pd.concat(results, ignore_index=True)
        st.success("Busca Finalizada!")
        st.dataframe(df_final, use_container_width=True)
        
        out = BytesIO()
        with pd.ExcelWriter(out, engine='xlsxwriter') as writer:
            df_final.to_excel(writer, index=False)
        
        st.download_button("📥 Baixar Consolidado (Excel)", out.getvalue(), "taxas_di.xlsx")
    else:
        st.warning("Nenhum dado retornado (verifique se são dias úteis ou feriados).")
