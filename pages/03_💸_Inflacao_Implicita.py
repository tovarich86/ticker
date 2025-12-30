import streamlit as st
import pandas as pd
from io import BytesIO
import sys
import os

# Garante que o Python encontre a pasta src
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

try:
    from src.treasury_service import carregar_dados_tesouro, calcular_inflacao_implicita, CSV_TESOURO_URL
except ImportError:
    st.error("Erro crítico: Não foi possível importar `src.treasury_service`. Verifique se o arquivo existe.")
    st.stop()

st.set_page_config(page_title="Inflação Implícita", layout="wide")
st.title("📊 Cálculo da Inflação Implícita")

st.markdown("""
A **Inflação Implícita** (ou *Break-even Inflation*) representa a média da inflação esperada pelo mercado para um determinado prazo.
Ela é obtida através da diferença (spread) entre as taxas dos títulos **Prefixados** (Nominal) e **Tesouro IPCA+** (Real).
""")

# --- BLOCO DE SEGURANÇA GERAL ---
try:
    # 1. TENTATIVA DE DOWNLOAD AUTOMÁTICO
    with st.spinner("Conectando ao Tesouro Nacional (pode levar alguns segundos)..."):
        df_raw = carregar_dados_tesouro()

    # 2. SE FALHAR (DATAFRAME VAZIO), ACIONA O PLANO B
    if df_raw.empty:
        st.warning("⚠️ O sistema do Tesouro Direto não respondeu ou bloqueou o download automático.")
        st.markdown("### 📂 Solução Manual")
        st.markdown(f"1. Baixe o arquivo **PrecoTaxaTesouroDireto.csv** [neste link oficial]({CSV_TESOURO_URL}).")
        st.markdown("2. Faça o upload do arquivo abaixo:")
        
        arquivo_manual = st.file_uploader("Arraste o arquivo CSV aqui", type=['csv'])
        
        if arquivo_manual:
            df_raw = carregar_dados_tesouro(arquivo_manual)
            if df_raw.empty:
                st.error("O arquivo enviado parece inválido ou vazio.")
                st.stop()
            else:
                st.success("Arquivo carregado com sucesso!")
        else:
            st.info("Aguardando upload para continuar...")
            st.stop()

    # 3. SELEÇÃO DE DATA E CÁLCULO
    if not df_raw.empty:
        # Garante que a coluna de data está correta
        df_raw["Data Base"] = pd.to_datetime(df_raw["Data Base"], errors='coerce')
        df_raw = df_raw.dropna(subset=["Data Base"])
        
        datas_disponiveis = df_raw["Data Base"].sort_values(ascending=False).unique()
        
        st.divider()
        col1, col2 = st.columns([1, 2])
        
        with col1:
            data_selecionada = st.selectbox(
                "📅 Data de Referência:",
                options=datas_disponiveis,
                format_func=lambda x: x.strftime("%d/%m/%Y"),
                index=0
            )

        if data_selecionada:
            with st.spinner("Calculando curvas..."):
                df_resultado, erro = calcular_inflacao_implicita(df_raw, data_selecionada)

            if erro:
                st.warning(f"⚠️ {erro}")
            else:
                # --- EXIBIÇÃO DA FÓRMULA (NOVA SEÇÃO) ---
                with st.expander("📝 Metodologia de Cálculo (Equação de Fisher)", expanded=True):
                    col_f1, col_f2 = st.columns([1, 1])
                    with col_f1:
                        st.markdown("O cálculo utiliza a relação entre juros nominais e reais:")
                        st.latex(r"""
                        \text{Inflação Implícita} = \left( \frac{1 + \text{Taxa Prefixada}}{1 + \text{Taxa IPCA+}} \right) - 1
                        """)
                    with col_f2:
                        st.info("""
                        **Lógica:**
                        1. Selecionamos os títulos **Prefixados** disponíveis na data.
                        2. Cruzamos com os títulos **IPCA+** de vencimento equivalente (usando interpolação).
                        3. A diferença entre o que o mercado paga fixo e o que paga acima da inflação é a **Inflação Esperada**.
                        """)

                # --- FORMATAÇÃO VISUAL ---
                df_show = df_resultado.copy()
                cols_data = ["Data Base", "Vencimento Prefixado", "Vencimento IPCA+ Ref"]
                for col in cols_data:
                    df_show[col] = df_show[col].dt.strftime("%d/%m/%Y")
                
                # Formatação percentual
                cols_pct = ["Inflação Implícita (%)", "Taxa Prefixada", "Taxa IPCA+"]
                for col in cols_pct:
                    df_show[col] = df_show[col].map("{:.2f}%".format)

                st.subheader(f"Resultados para {data_selecionada.strftime('%d/%m/%Y')}")
                st.dataframe(df_show, use_container_width=True)

                # Botão Excel
                output = BytesIO()
                with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                    df_resultado.to_excel(writer, index=False, sheet_name="Inflacao_Implicita")
                
                st.download_button(
                    label="📥 Baixar Resultado em Excel",
                    data=output.getvalue(),
                    file_name=f"inflacao_implicita_{data_selecionada.strftime('%Y%m%d')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

except Exception as e:
    st.error(f"Ocorreu um erro inesperado na aplicação: {e}")
    st.markdown("Tente recarregar a página.")
