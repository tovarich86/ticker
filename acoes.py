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
    * **Cotações:** Oficial da B3 (COTAHIST) com Open, High, Low, Close, Average, VWAP.
    * **Proventos:** Dividendos e Bonificações direto da API da B3.
    * **Tickers:** Suporte a ações brasileiras e internacionais (Yahoo Finance).

    👉 *Acesse no menu lateral: **Busca de Ativos***
    """)

    st.header("📊 Volatilidade")
    st.markdown("""
    **Análise de Volatilidade para Precificação de Opções**

    Calcula a volatilidade realizada de ações por múltiplas metodologias estatísticas.
    * **Metodologias:** Histórica (C-C), Parkinson, Garman-Klass, Rogers-Satchell, Yang-Zhang, EWMA e GARCH(1,1).
    * **Janelas:** Volatilidade por janelas móveis (252d, 504d, 756d...) prontas para uso em Black-Scholes, Binomial e Monte Carlo.
    * **Auditoria:** Export Excel com cotações e cálculos detalhados por ticker.

    👉 *Acesse no menu lateral: **Volatilidade***
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
    st.header("📈 TSR — Total Shareholder Return")
    st.markdown("""
    **Retorno Total ao Acionista (dados exclusivos da B3)**

    Calcula o TSR considerando variação de preço, dividendos/JCP e eventos corporativos.
    * **Preços:** Fechamento, Média ou VWAP calculados via COTAHIST oficial.
    * **Proventos:** Dividendos e JCP com ajuste de quantidade por data ex.
    * **Eventos corporativos:** Bonificações, desdobramentos e grupamentos com multiplicador conforme Manual B3.
    * **Ranking:** Comparativo entre múltiplos ativos com export Excel auditável.

    👉 *Acesse no menu lateral: **TSR***
    """)

    st.header("💸 Inflação Futura")
    st.markdown("""
    **Inflação Implícita (Tesouro Direto)**

    Inflação que o mercado está precificando a partir da taxa pré e pós fixada.
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
