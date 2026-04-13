import streamlit as st
import pandas as pd
import re
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs

st.set_page_config(page_title="FRE - CVM", layout="wide")

# URLs dos arquivos hospedados no GitHub
CSV_URL       = "https://github.com/ArthurModesto1/fre-itens/raw/main/fre_cia_aberta_2025.csv"
PLANOS_URL    = "https://github.com/ArthurModesto1/fre-itens/raw/main/tabela_consolidada_cvm_otimizado.xlsx"

DOWNLOAD_FILES = {
    "8.2":  "https://github.com/ArthurModesto1/fre-itens/raw/main/fre_cia_aberta_remuneracao_total_orgao_2025.csv",
    "8.3":  "https://github.com/ArthurModesto1/fre-itens/raw/main/fre_cia_aberta_remuneracao_variavel_2025.csv",
    "8.5":  "https://github.com/ArthurModesto1/fre-itens/raw/main/fre_cia_aberta_remuneracao_acao_2025.csv",
    "8.11": "https://github.com/ArthurModesto1/fre-itens/raw/main/fre_cia_aberta_acao_entregue_2025.csv",
}

MAPEAMENTO_QUADROS = {
    "8.1": "8030", "8.4": "8120", "8.6": "8180", "8.7": "8210",
    "8.8": "8240", "8.9": "8270", "8.10": "8300", "8.12": "8360",
}

st.markdown("""
<style>
.fre-tabela-container {
    max-height: 420px;
    overflow-x: auto;
    overflow-y: auto;
    border-radius: 0.5rem;
    border: 1px solid #dde3ed;
}
.fre-tabela {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.83rem;
    min-width: 800px;
}
.fre-tabela th {
    position: sticky;
    top: 0;
    background-color: #f0f4fa;
    text-align: center;
    padding: 8px;
    z-index: 1;
}
.fre-tabela td {
    padding: 6px 10px;
    text-align: center;
    border-top: 1px solid #e8edf5;
    white-space: nowrap;
}
.fre-tabela-container::-webkit-scrollbar { width: 7px; height: 7px; }
.fre-tabela-container::-webkit-scrollbar-track { background: #f5f7fa; border-radius: 8px; }
.fre-tabela-container::-webkit-scrollbar-thumb { background: #c5cfe0; border-radius: 8px; }
</style>
""", unsafe_allow_html=True)

st.title("📄 Visualizador FRE — CVM")
st.markdown("Consulta de documentos do **Formulário de Referência** de companhias abertas (itens de remuneração — Seção 8).")
st.divider()


@st.cache_data(ttl=3600)
def load_data():
    def norm(name):
        if pd.isna(name):
            return None
        name = str(name).upper().strip()
        name = re.sub(r"\s+(S\.?A\.?|S/A|SA)$", " S.A.", name)
        return name

    df_fre = pd.read_csv(CSV_URL, sep=';', dtype=str, encoding="latin-1")
    df_fre["DENOM_CIA"] = df_fre["DENOM_CIA"].apply(norm)

    df_planos = pd.read_excel(PLANOS_URL, dtype=str)
    df_planos["Empresa"] = df_planos["Empresa"].apply(norm)

    return df_fre, df_planos


with st.spinner("Carregando base FRE..."):
    df, df_planos = load_data()

df = df.sort_values(by=["DENOM_CIA", "VERSAO"], ascending=[True, False])
empresas_unicas = sorted(set(df["DENOM_CIA"].dropna()) | set(df_planos["Empresa"].dropna()))

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("🔎 Filtros")
    selected_company = st.selectbox("Empresa", empresas_unicas)
    selected_item    = st.radio(
        "Item do FRE",
        ["8.1","8.2","8.3","8.4","8.5","8.6","8.7","8.8","8.9","8.10","8.11","8.12"],
    )

# ── Cabeçalho da empresa ──────────────────────────────────────────────────────
st.subheader(f"🏢 {selected_company}")
st.write(f"Item selecionado: **{selected_item}**")

df_filtered = df[df["DENOM_CIA"] == selected_company]

# ── Items com dados para download (8.2, 8.3, 8.5, 8.11) ───────────────────────
if selected_item in DOWNLOAD_FILES:
    st.info(f"📥 O item {selected_item} permite download dos dados filtrados por empresa.")

    try:
        df_dl = pd.read_csv(DOWNLOAD_FILES[selected_item], sep=';', encoding="latin-1", dtype=str)
        col_name = "Nome_Companhia"
        df_dl[col_name] = df_dl[col_name].str.upper().str.strip()
        df_dl_filtered = df_dl[df_dl[col_name].str.contains(selected_company, na=False)]

        if not df_dl_filtered.empty:
            csv_bytes = df_dl_filtered.to_csv(index=False, sep=';', encoding="latin-1").encode("latin-1")
            st.download_button(
                label="💾 Baixar CSV filtrado",
                data=csv_bytes,
                file_name=f"item_{selected_item}_{selected_company}.csv",
                mime="text/csv",
            )

            st.markdown("#### 📊 Prévia dos dados")
            html_prev = df_dl_filtered.head(10).to_html(index=False, classes='fre-tabela', escape=False)
            st.markdown(f'<div class="fre-tabela-container">{html_prev}</div>', unsafe_allow_html=True)
        else:
            st.warning("Nenhum dado encontrado para esta empresa neste item.")

    except Exception as e:
        st.error(f"Erro ao processar item {selected_item}: {e}")

# ── Items com link direto para o RAD/CVM ─────────────────────────────────────
else:
    def extract_doc_number(url):
        if pd.isna(url):
            return None
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        return params.get("NumeroSequencialDocumento", [None])[0]

    document_url = df_filtered.iloc[0]["LINK_DOC"] if not df_filtered.empty else None

    if document_url:
        doc_number = extract_doc_number(document_url)
        if doc_number:
            codigo_quadro = MAPEAMENTO_QUADROS.get(selected_item, "8030")
            fre_url = (
                f"https://www.rad.cvm.gov.br/ENET/frmExibirArquivoFRE.aspx"
                f"?NumeroSequencialDocumento={doc_number}"
                f"&CodigoGrupo=8000&CodigoQuadro={codigo_quadro}"
            )
            st.markdown("#### 📄 Documento FRE")
            st.link_button("🔗 Abrir documento na CVM", fre_url)
        else:
            st.warning("Não foi possível extrair o número do documento.")
    else:
        st.warning("Nenhum documento FRE encontrado para esta empresa.")

# ── Planos de remuneração ─────────────────────────────────────────────────────
st.divider()
st.subheader("📋 Planos de Remuneração")

planos = df_planos[df_planos["Empresa"] == selected_company]

if not planos.empty:
    planos = planos.copy()
    if "Link" in planos.columns:
        planos["Link"] = planos["Link"].apply(
            lambda x: f'<a href="{x}" target="_blank">Abrir Documento</a>'
        )
    html_planos = planos.to_html(escape=False, index=False, justify="center", classes='fre-tabela')
    st.markdown(f'<div class="fre-tabela-container">{html_planos}</div>', unsafe_allow_html=True)
else:
    st.info("Nenhum plano de remuneração cadastrado para esta empresa.")

st.caption("Fonte: CVM — Formulário de Referência (FRE) | Dados: github.com/ArthurModesto1/fre-itens")
