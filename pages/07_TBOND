import streamlit as st
import pandas as pd
import pandas_datareader.data as web
from io import BytesIO
from datetime import datetime, timedelta

st.set_page_config(page_title="US Treasuries", layout="wide")
st.title("🇺🇸 T-Bonds e T-Notes (Taxas Oficiais do Tesouro)")

st.markdown("Consulta de dados históricos (Constant Maturity) espelhando a base oficial do US Treasury via FRED.")

# Tickers oficiais do FRED para as taxas do US Treasury
TICKERS_FRED = {
    "10-Year T-Note": "DGS10",
    "30-Year T-Bond": "DGS30",
    "5-Year T-Note": "DGS5",
    "3-Month T-Bill": "DGS3MO"
}

col1, col2 = st.columns([1, 2])

with col1:
    ativos_selecionados = st.multiselect(
        "Selecione os Títulos:",
        options=list(TICKERS_FRED.keys()),
        default=["10-Year T-Note"]
    )

with col2:
    col_date1, col_date2 = st.columns(2)
    with col_date1:
        data_inicio = st.date_input("Data de Início", datetime.now() - timedelta(days=30))
    with col_date2:
        data_fim = st.date_input("Data de Fim", datetime.now())

if st.button("Buscar Taxas Oficiais", type="primary"):
    if not ativos_selecionados:
        st.warning("Selecione pelo menos um título.")
        st.stop()
        
    resultados = []
    bar = st.progress(0)
    
    with st.spinner("Consultando dados oficiais no FRED (Federal Reserve)..."):
        for i, ativo in enumerate(ativos_selecionados):
            ticker_fred = TICKERS_FRED[ativo]
            
            try:
                # Busca os dados no FRED
                df_fred = web.DataReader(ticker_fred, 'fred', data_inicio, data_fim)
                
                if not df_fred.empty:
                    df = df_fred.reset_index()
                    df = df.rename(columns={'DATE': 'Data', ticker_fred: 'Taxa'})
                    
                    # Remove dias sem taxa (feriados e finais de semana vêm como NaN no FRED)
                    df = df.dropna()
                    
                    df_clean = pd.DataFrame()
                    df_clean['DATA'] = df['Data'].dt.strftime("%d/%m/%Y")
                    df_clean['VENCIMENTO'] = ativo
                    df_clean['TAXA_YIELD (%)'] = df['Taxa'].round(4)
                    
                    # Link de auditoria direto para o gráfico do FRED (que cita o US Treasury como fonte)
                    df_clean['FONTE_AUDITORIA'] = f"https://fred.stlouisfed.org/series/{ticker_fred}"
                    
                    resultados.append(df_clean)
                    
            except Exception as e:
                st.error(f"Erro ao buscar {ativo}: {e}")
                
            bar.progress((i + 1) / len(ativos_selecionados))

    if resultados:
        df_final = pd.concat(resultados, ignore_index=True)
        df_final = df_final.sort_values(by=['DATA', 'VENCIMENTO'], ascending=[False, True])
        
        st.success("Busca Finalizada com sucesso!")
        
        st.dataframe(
            df_final, 
            use_container_width=True,
            column_config={
                "FONTE_AUDITORIA": st.column_config.LinkColumn(
                    "Link para Validação",
                    help="Dados oficiais via Federal Reserve Economic Data (FRED).",
                    display_text="Ver Fonte Oficial (FRED)"
                )
            }
        )
        
        out = BytesIO()
        with pd.ExcelWriter(out, engine='xlsxwriter') as writer:
            df_final.to_excel(writer, index=False)
        
        st.download_button(
            label="📥 Baixar Evidência (Excel)", 
            data=out.getvalue(), 
            file_name=f"historico_treasury_oficial_{datetime.now().strftime('%Y%m%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
