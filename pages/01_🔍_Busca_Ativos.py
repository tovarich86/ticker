import streamlit as st
import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta
import sys, os

# Importa serviços
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src import ticker_service

st.set_page_config(page_title="Busca de Ativos", layout="wide")
st.title("🔍 Busca Híbrida de Ativos (B3 + Yahoo)")

st.markdown("""
**Fontes:**
* **B3:** Cotações (COTAHIST) e Proventos (API Listadas).
* **Yahoo Finance:** Dados internacionais e Fechamento Ajustado.
""")

# --- INPUTS NA PÁGINA PRINCIPAL (Layout Original) ---
col1, col2 = st.columns(2)

with col1:
    tickers_input = st.text_input(
        "Tickers (ex: PETR4, AAPL, ITUB4):", 
        placeholder="Separe por vírgula",
        help="Misture ativos BR e internacionais"
    )

with col2:
    tipos_dados = st.multiselect(
        "Dados Desejados:", 
        ["Cotações (OHLCV)", "Dividendos", "Bonificações"], 
        default=["Cotações (OHLCV)"]
    )

col3, col4 = st.columns(2)

# Definindo datas padrão
dt_hoje = datetime.now()
dt_ini_padrao = dt_hoje - timedelta(days=10)

with col3:
    # Mantive date_input pois é mais robusto que text_input, mas no layout original
    dt_ini = st.date_input("Data Início:", value=dt_ini_padrao, format="DD/MM/YYYY")

with col4:
    dt_fim = st.date_input("Data Fim:", value=dt_hoje, format="DD/MM/YYYY")

st.markdown("---")
btn_buscar = st.button("Executar Busca", type="primary")

# --- LÓGICA DE PROCESSAMENTO ---
if btn_buscar:
    if not tickers_input:
        st.warning("Digite pelo menos um ticker.")
        st.stop()

    # Carrega banco de empresas (cacheado)
    df_empresas = ticker_service.carregar_empresas()
    
    # Containers de resultados
    tabs = st.tabs(["📊 Cotações", "💰 Dividendos", "🎁 Bonificações"])
    
    with st.spinner("Processando dados (isso pode levar alguns segundos)..."):
        # 1. COTAÇÕES
        if "Cotações (OHLCV)" in tipos_dados:
            res_cotacoes, erros = ticker_service.buscar_cotacoes_hibrido(
                tickers_input, 
                dt_ini.strftime("%d/%m/%Y"), 
                dt_fim.strftime("%d/%m/%Y"), 
                df_empresas
            )
            
            with tabs[0]:
                if erros:
                    for e in erros: st.error(e)
                
                if res_cotacoes:
                    df_all = pd.concat(res_cotacoes.values(), ignore_index=True)
                    st.dataframe(df_all, use_container_width=True)
                    
                    # Botão Excel
                    out = BytesIO()
                    with pd.ExcelWriter(out, engine='xlsxwriter') as writer:
                        df_all.to_excel(writer, index=False)
                    st.download_button("Baixar Cotações (XLSX)", out.getvalue(), "cotacoes.xlsx")
                else:
                    st.info("Nenhuma cotação encontrada.")

        # 2. PROVENTOS
        tickers_list = [t.strip().upper() for t in tickers_input.split(',') if t.strip()]
        dfs_div = []
        dfs_bon = []
        
        for t in tickers_list:
            if "Dividendos" in tipos_dados:
                d = ticker_service.buscar_proventos_b3(t, "Dividendos", df_empresas, pd.to_datetime(dt_ini), pd.to_datetime(dt_fim))
                if not d.empty: 
                    d.insert(0, 'Ticker', t)
                    dfs_div.append(d)
            
            if "Bonificações" in tipos_dados:
                b = ticker_service.buscar_proventos_b3(t, "Bonificacoes", df_empresas, pd.to_datetime(dt_ini), pd.to_datetime(dt_fim))
                if not b.empty: 
                    b.insert(0, 'Ticker', t)
                    dfs_bon.append(b)

        with tabs[1]:
            if dfs_div:
                df_div_final = pd.concat(dfs_div)
                st.dataframe(df_div_final, use_container_width=True)
                
                out_div = BytesIO()
                with pd.ExcelWriter(out_div, engine='xlsxwriter') as writer:
                    df_div_final.to_excel(writer, index=False)
                st.download_button("Baixar Dividendos (XLSX)", out_div.getvalue(), "dividendos.xlsx")
                
            else: st.caption("Sem dividendos no período.")

        with tabs[2]:
            if dfs_bon:
                df_bon_final = pd.concat(dfs_bon)
                st.dataframe(df_bon_final, use_container_width=True)
                
                out_bon = BytesIO()
                with pd.ExcelWriter(out_bon, engine='xlsxwriter') as writer:
                    df_bon_final.to_excel(writer, index=False)
                st.download_button("Baixar Bonificações (XLSX)", out_bon.getvalue(), "bonificacoes.xlsx")
                
            else: st.caption("Sem bonificações no período.")
