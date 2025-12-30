# Arquivo: pages/04_🧮_Calculadora_Cidada.py
import streamlit as st
import pandas as pd
from datetime import datetime
import sys
import os

# Setup de importação
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.ibge_service import carregar_dados_ipca, calcular_correcao_ipca

st.set_page_config(page_title="Calculadora IPCA", layout="centered")
st.title('🧮 Calculadora de Correção pelo IPCA')

# Carregamento Inicial
if 'df_ipca' not in st.session_state:
    with st.spinner("Carregando dados do IBGE..."):
        st.session_state.df_ipca = carregar_dados_ipca()

df_ipca = st.session_state.df_ipca

if df_ipca.empty:
    st.error("Erro ao conectar com o IBGE.")
    st.stop()

# Limites de Data
data_min = df_ipca['data'].min().date()
data_max = df_ipca['data'].max().date()

# Formulário
st.info(f"Dados disponíveis de {data_min.strftime('%m/%Y')} até {data_max.strftime('%m/%Y')}")

col1, col2 = st.columns(2)
with col1:
    data_inicial = st.date_input('Data Inicial', value=data_min, min_value=data_min, max_value=data_max)
with col2:
    data_final = st.date_input('Data Final', value=data_max, min_value=data_min, max_value=data_max)

valor_inicial = st.number_input('Valor a corrigir (R$)', min_value=0.0, value=1000.0, step=100.0)
taxa_input = st.text_input('Taxa Adicional Anual (%) (Opcional)', value='0,0')

# Conversão da taxa
try:
    taxa_aa = float(taxa_input.replace(',', '.')) / 100
except:
    taxa_aa = 0.0

# Botão de Cálculo
if st.button("Calcular Correção", type="primary"):
    dt_ini = datetime(data_inicial.year, data_inicial.month, 1)
    dt_fim = datetime(data_final.year, data_final.month, 1)

    if dt_ini >= dt_fim:
        st.warning("A Data Inicial deve ser anterior à Data Final.")
    else:
        acumulado, df_memoria, valor_corr, idx_base = calcular_correcao_ipca(df_ipca, dt_ini, dt_fim, valor_inicial)
        
        if acumulado is None:
            st.error("Dados insuficientes para o período selecionado (verifique se há índice anterior à data inicial).")
        else:
            # Cálculo da taxa prefixada extra
            meses = (dt_fim.year - dt_ini.year) * 12 + (dt_fim.month - dt_ini.month)
            fator_pre = (1 + taxa_aa) ** (meses / 12) if taxa_aa else 1.0
            valor_final_total = valor_corr * fator_pre
            pct_total = (valor_final_total / valor_inicial) - 1

            # Exibição dos Resultados
            st.divider()
            col_res1, col_res2 = st.columns(2)
            
            with col_res1:
                st.metric("Valor Original", f"R$ {valor_inicial:,.2f}")
                st.metric("IPCA Acumulado", f"{acumulado*100:.2f}%")
            
            with col_res2:
                st.metric("Valor Corrigido (IPCA)", f"R$ {valor_corr:,.2f}")
                if taxa_aa > 0:
                    st.metric(f"Total (+ {taxa_aa*100}% a.a.)", f"R$ {valor_final_total:,.2f}", delta=f"{pct_total*100:.2f}%")

            # Tabela Memória de Cálculo
            with st.expander("Ver Memória de Cálculo Mensal"):
                df_exibicao = df_memoria[['data', 'valor', 'var_mes']].copy()
                df_exibicao['data'] = df_exibicao['data'].dt.strftime('%m/%Y')
                df_exibicao['var_mes'] = df_exibicao['var_mes'].map(lambda x: f"{x*100:.2f}%")
                st.dataframe(df_exibicao, use_container_width=True)
