# Arquivo: Home.py
import streamlit as st

# Configuração da Página Principal
st.set_page_config(
    page_title="Portal Financeiro B3/Tesouro",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': 'https://github.com/tovarich86',
        'Report a bug': "https://github.com/tovarich86",
        'About': "# Portal Financeiro Integrado\nDados da B3, Tesouro Direto e IBGE."
    }
)

# Cabeçalho
st.title("Portal de Dados Financeiros")
st.markdown("#### ferramentas para análise de dados do mercado brasileiro.")
st.divider()

# Layout em Colunas para apresentar as ferramentas
col1, col2 = st.columns(2)

with col1:
    st.header("🔍 Mercado de Ações")
    st.markdown("""
    **Busca Híbrida de Ativos (B3 + Yahoo)**
    
    Dados de tickers nacionais e internacionais.
    * **Cotações:** Oficial da B3 (COTAHIST).
    * **Proventos:** Dividendos e Bonificações direto da API da B3.
    * **Tickers:** Suporte a ações brasileiras e internacionais (Yahoo finance).
    
    👉 *Acesse no menu lateral: **Busca de Ativos***
    """)

    st.header("📉 Juros Futuros (DI1)")
    st.markdown("""
    **Curva de Juros DI**
    
    Expectativa do mercado para a taxa Selic no futuro.
    * **Fonte Oficial:** Dados de Preços Referenciais da B3.
    * **Histórico:** Permite baixar a curva de qualquer data passada.
      
    👉 *Acesse no menu lateral: **Taxas DI1***
    """)

with col2:
    st.header("💸 Renda Fixa & Inflação")
    st.markdown("""
    **Inflação Implícita (Tesouro Direto)**
    
   Inflação que o mercado está precificando a  partir da taxa pré e pós fixada.
    * **Metodologia:** Diferença entre Taxa Prefixada e Taxa IPCA+ (Fisher).
    * **Interpolação:** Cruzamento de vértices de vencimento.
    
    👉 *Acesse no menu lateral: **Inflação Implícita***
    """)

    st.header("🧮 Calculadora IPCA")
    st.markdown("""
    **Correção Monetária (IPCA)**
    
    Atualize valores monetários pela inflação oficial.
    * **Dados:** API Oficial do SIDRA/IBGE.
    * **Flexibilidade:** Permite adicionar juros prefixados ao cálculo.
       
    👉 *Acesse no menu lateral: **Calculadora IPCA***
    """)

st.divider()

# Rodapé Informativo
st.info("""
**Como usar:**
Utilize o menu à esquerda (Sidebar) para navegar entre as diferentes ferramentas.
Todas as ferramentas consomem dados públicos em tempo real (ou com o delay padrão das fontes).
""")

st.caption("Desenvolvido com Python & Streamlit | Fontes: B3, Tesouro Nacional, IBGE e Yahoo Finance.")
