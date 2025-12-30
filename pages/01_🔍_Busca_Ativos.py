# Arquivo: pages/01_🔍_Busca_Ativos.py
import streamlit as st
import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta
import sys, os

# Setup de importação do src
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src import ticker_service

st.set_page_config(page_title="Busca de Ativos", layout="wide")
st.title("🔍 Busca Híbrida de Ativos (B3 + Yahoo)")

# --- 1. CARREGAMENTO DE EMPRESAS (COM FALLBACK) ---
# Tenta carregar automaticamente do GitHub
df_empresas = ticker_service.carregar_empresas()

# Se falhar (DataFrame vazio), pede upload manual
if df_empresas.empty:
    st.warning("⚠️ Não foi possível baixar a lista de empresas do GitHub. Por favor, carregue o arquivo 'empresas_b3.xlsx' manualmente.")
    arquivo_manual = st.file_uploader("Upload da Planilha de Empresas (Excel)", type=["xlsx"])
    
    if arquivo_manual:
        df_empresas = ticker_service.carregar_empresas(arquivo_manual)
        if not df_empresas.empty:
            st.success(f"Arquivo carregado com sucesso! {len(df_empresas)} empresas identificadas.")
        else:
            st.error("Erro ao ler o arquivo enviado. Verifique se é um Excel válido da B3.")
            st.stop()
    else:
        st.info("Aguardando upload para prosseguir...")
        st.stop() # Para a execução aqui até o usuário enviar o arquivo

# --- 2. INPUTS DE BUSCA ---
col1, col2 = st.columns(2)
with col1:
    tickers_input = st.text_input("Tickers (ex: PETR4, AAPL, ITUB4):", placeholder="Separe por vírgula")
with col2:
    tipos_dados = st.multiselect("Dados:", ["Preços", "Dividendos", "Bonificações"], default=["Preços"])

col3, col4 = st.columns(2)
dt_hoje = datetime.now()
with col3:
    dt_ini = st.date_input("Início:", value=dt_hoje - timedelta(days=10), format="DD/MM/YYYY")
with col4:
    dt_fim = st.date_input("Fim:", value=dt_hoje, format="DD/MM/YYYY")

st.markdown("---")
btn_buscar = st.button("Executar Busca", type="primary")

# --- 3. LÓGICA DE PROCESSAMENTO ---
if btn_buscar:
    if not tickers_input:
        st.warning("Digite pelo menos um ticker.")
        st.stop()

    tabs = st.tabs(["📊 Cotações", "💰 Dividendos", "🎁 Bonificações"])
    
    with st.spinner("Processando dados..."):
        # COTAÇÕES
        if "Preços" in tipos_dados:
            # Chama a função blindada do service
            res_cotacoes, erros = ticker_service.buscar_dados_hibrido(
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
                    
                    out = BytesIO()
                    with pd.ExcelWriter(out, engine='xlsxwriter') as writer:
                        df_all.to_excel(writer, index=False)
                    st.download_button("Baixar Cotações (XLSX)", out.getvalue(), "cotacoes.xlsx")
                else:
                    st.info("Nenhuma cotação encontrada.")

        # PROVENTOS
        tickers_list = [t.strip().upper() for t in tickers_input.split(',') if t.strip()]
        dfs_div = []
        dfs_bon = []
        
        for t in tickers_list:
            if "Dividendos" in tipos_dados:
                d = ticker_service.buscar_dividendos_b3(t, df_empresas, pd.to_datetime(dt_ini), pd.to_datetime(dt_fim))
                if not d.empty: 
                    d.insert(0, 'Ticker', t)
                    dfs_div.append(d)
            
            if "Bonificações" in tipos_dados:
                b = ticker_service.buscar_bonificacoes_b3(t, df_empresas, pd.to_datetime(dt_ini), pd.to_datetime(dt_fim))
                if not b.empty: 
                    b.insert(0, 'Ticker', t)
                    dfs_bon.append(b)

        with tabs[1]:
            if dfs_div:
                final_div = pd.concat(dfs_div)
                st.dataframe(final_div, use_container_width=True)
            else: st.caption("Sem dividendos no período.")

        with tabs[2]:
            if dfs_bon:
                final_bon = pd.concat(dfs_bon)
                st.dataframe(final_bon, use_container_width=True)
            else: st.caption("Sem bonificações no período.")
