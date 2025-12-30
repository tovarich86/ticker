import streamlit as st
import pandas as pd
from datetime import datetime
import sys
import os

# Setup de importação para encontrar o módulo src
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.ibge_service import carregar_dados_ipca, calcular_correcao_ipca

st.set_page_config(page_title="Calculadora IPCA", layout="wide")
st.title('🧮 Calculadora de Correção pelo IPCA')

# --- 1. CARREGAMENTO DE DADOS (Cacheado) ---
if 'df_ipca' not in st.session_state:
    with st.spinner("Carregando dados oficiais do IBGE..."):
        st.session_state.df_ipca = carregar_dados_ipca()

df_ipca = st.session_state.df_ipca

if df_ipca.empty:
    st.error("Erro ao conectar com o IBGE. Tente novamente mais tarde.")
    st.stop()

# --- 2. INPUTS (Parâmetros) ---
# Limites de Data disponíveis no IBGE
data_min = df_ipca['data'].min().date()
data_max = df_ipca['data'].max().date()

st.info(f"Dados do IBGE disponíveis de **{data_min.strftime('%m/%Y')}** até **{data_max.strftime('%m/%Y')}**")

with st.container():
    col_input1, col_input2, col_input3, col_input4 = st.columns(4)
    
    with col_input1:
        data_inicial = st.date_input('Data Inicial', value=data_min, min_value=data_min, max_value=data_max)
    with col_input2:
        data_final = st.date_input('Data Final', value=data_max, min_value=data_min, max_value=data_max)
    with col_input3:
        valor_inicial = st.number_input('Valor a Corrigir (R$)', min_value=0.0, value=1000.0, step=100.0, format="%.2f")
    with col_input4:
        taxa_input = st.text_input('Taxa Adicional Anual (%)', value='0,0', help="Ex: Juros de 6% a.a. além da inflação")

# Conversão da taxa input (string -> float)
try:
    taxa_aa = float(taxa_input.replace(',', '.')) / 100
except:
    taxa_aa = 0.0

st.divider()

# --- 3. CÁLCULO E VISUALIZAÇÃO ---
if st.button("Calcular Correção", type="primary", use_container_width=True):
    dt_ini = datetime(data_inicial.year, data_inicial.month, 1)
    dt_fim = datetime(data_final.year, data_final.month, 1)

    if dt_ini >= dt_fim:
        st.warning("⚠️ A Data Inicial deve ser anterior à Data Final.")
    else:
        # Cálculo IPCA (src/ibge_service.py)
        ipca_acumulado, df_memoria, valor_corrigido_ipca, idx_base = calcular_correcao_ipca(
            df_ipca, dt_ini, dt_fim, valor_inicial
        )
        
        if ipca_acumulado is None:
            st.error("Dados insuficientes para o período selecionado.")
        else:
            # Cálculos Adicionais
            meses = (dt_fim.year - dt_ini.year) * 12 + (dt_fim.month - dt_ini.month)
            
            # Fator Pré-fixado (Juros Compostos pro rata temporis)
            fator_pre = (1 + taxa_aa) ** (meses / 12)
            pct_pre_periodo = fator_pre - 1
            
            # Valor Final Combinado: Valor * (1 + IPCA) * (1 + TaxaPre)
            # Ou: Valor_Corrigido_IPCA * Fator_Pre
            valor_final_total = valor_corrigido_ipca * fator_pre
            
            # Variação Total Percentual
            pct_total_combinado = (valor_final_total / valor_inicial) - 1

            # --- LAYOUT SOLICITADO: COLUNAS SEPARADAS ---
            col_monetaria, col_percentual = st.columns(2)

            # Coluna 1: Valores Monetários (R$)
            with col_monetaria:
                st.subheader("💰 Resultado Monetário")
                st.markdown("---")
                st.metric(
                    label="Valor Original", 
                    value=f"R$ {valor_inicial:,.2f}"
                )
                
                st.metric(
                    label="Valor Corrigido (Apenas IPCA)", 
                    value=f"R$ {valor_corrigido_ipca:,.2f}",
                    delta=f"R$ {valor_corrigido_ipca - valor_inicial:,.2f}",
                    help="Valor inicial atualizado apenas pela inflação."
                )
                
                if taxa_aa > 0:
                    st.metric(
                        label="Valor Final (IPCA + Taxa Adicional)", 
                        value=f"R$ {valor_final_total:,.2f}",
                        delta=f"R$ {valor_final_total - valor_inicial:,.2f}",
                        help="Valor final incluindo o ganho real da taxa adicional."
                    )

            # Coluna 2: Variações Percentuais (%)
            with col_percentual:
                st.subheader("📈 Composição das Taxas")
                st.markdown("---")
                
                st.metric(
                    label="IPCA Acumulado (Período)", 
                    value=f"{ipca_acumulado*100:.2f}%"
                )
                
                if taxa_aa > 0:
                    st.metric(
                        label=f"Taxa Adicional Acumulada ({taxa_aa*100}% a.a.)", 
                        value=f"{pct_pre_periodo*100:.2f}%",
                        help=f"Equivalente a {meses} meses de juros compostos."
                    )
                    
                    st.metric(
                        label="Variação Total (IPCA + Taxa)", 
                        value=f"{pct_total_combinado*100:.2f}%",
                        delta="Juros sobre Juros",
                        delta_color="off",
                        help="Não é uma soma simples! É o efeito composto das duas taxas."
                    )

            # --- FÓRMULA MATEMÁTICA ---
            if taxa_aa > 0:
                st.markdown("### 📝 Fórmula do Efeito Conjunto")
                st.info("O retorno total não é a soma das taxas, mas sim a multiplicação dos fatores (Juros Compostos).")
                
                # Exibição LaTeX
                st.latex(r"""
                (1 + \text{Total}) = (1 + \text{IPCA}) \times (1 + \text{Taxa Adicional})^{\frac{n}{12}}
                """)
                
                st.markdown(f"""
                **Aplicando aos seus dados:**
                * Fator IPCA: $(1 + {ipca_acumulado:.4f}) = {1+ipca_acumulado:.4f}$
                * Fator Taxa Adicional: $(1 + {taxa_aa:.4f})^{{{meses}/12}} = {fator_pre:.4f}$
                
                $$
                {1+ipca_acumulado:.4f} \\times {fator_pre:.4f} = {1+pct_total_combinado:.4f} \\implies \\text{{{pct_total_combinado*100:.2f}\%}}
                $$
                """)

            # --- TABELA DE MEMÓRIA (Expansível) ---
            with st.expander("Ver Memória de Cálculo Mensal (IPCA)"):
                df_exibicao = df_memoria[['data', 'valor', 'var_mes']].copy()
                df_exibicao.rename(columns={
                    'data': 'Mês/Ano', 
                    'valor': 'Índice IBGE', 
                    'var_mes': 'Variação Mensal'
                }, inplace=True)
                
                df_exibicao['Mês/Ano'] = df_exibicao['Mês/Ano'].dt.strftime('%m/%Y')
                df_exibicao['Variação Mensal'] = df_exibicao['Variação Mensal'].map(lambda x: f"{x*100:.2f}%")
                
                st.dataframe(df_exibicao, use_container_width=True)
