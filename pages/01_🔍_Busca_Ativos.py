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
df_empresas = ticker_service.carregar_empresas()

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
        st.stop()

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
        # 1. Busca de Cotações
        res_cotacoes = {}
        if "Preços" in tipos_dados:
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
                    df_all_cot = pd.concat(res_cotacoes.values(), ignore_index=True)
                    st.dataframe(df_all_cot, use_container_width=True)
                else:
                    st.info("Nenhuma cotação encontrada.")

        # 2. Busca de Proventos e Eventos
        tickers_list = [t.strip().upper() for t in tickers_input.split(',') if t.strip()]
        dfs_div = []
        dfs_bon = []
        
        for t in tickers_list:
            is_b3 = ticker_service.parece_b3_ticker(t)
            t_inicio = pd.to_datetime(dt_ini)
            t_fim = pd.to_datetime(dt_fim)

            if "Dividendos" in tipos_dados:
                if is_b3:
                    d = ticker_service.buscar_dividendos_b3(t, df_empresas, t_inicio, t_fim)
                else:
                    d = ticker_service.buscar_dividendos_yf(t, t_inicio, t_fim)
                if not d.empty: dfs_div.append(d)

            if "Bonificações" in tipos_dados:
                if is_b3:
                    b = ticker_service.buscar_bonificacoes_b3(t, df_empresas, t_inicio, t_fim)
                else:
                    # Para ativos internacionais, assume-se "Bonificações" como Splits de ações
                    b = ticker_service.buscar_splits_yf(t, t_inicio, t_fim)
                if not b.empty: dfs_bon.append(b)

        # Exibição nas Tabs
        with tabs[1]:
            if dfs_div:
                st.dataframe(pd.concat(dfs_div), use_container_width=True)
            else: st.caption("Sem dividendos no período.")

        with tabs[2]:
            if dfs_bon:
                st.dataframe(pd.concat(dfs_bon), use_container_width=True)
            else: st.caption("Sem bonificações no período.")

        # 3. Lógica de Exportação Unificada
        if res_cotacoes or dfs_div or dfs_bon:
            out = BytesIO()
            with pd.ExcelWriter(out, engine='xlsxwriter') as writer:
                if res_cotacoes:
                    pd.concat(res_cotacoes.values(), ignore_index=True).to_excel(writer, index=False, sheet_name='Cotações')
                if dfs_div:
                    pd.concat(dfs_div).to_excel(writer, index=False, sheet_name='Dividendos')
                if dfs_bon:
                    pd.concat(dfs_bon).to_excel(writer, index=False, sheet_name='Bonificacoes')

            st.markdown("---")
            st.download_button(
                label="📥 Baixar Dados Completos (XLSX)",
                data=out.getvalue(),
                file_name=f"busca_ativos_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
