import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import streamlit as st
import pandas as pd
from src.lti.config import OUTORGAS
from src.lti.engine import calcular_outorga
from src.lti.excel_builder import gerar_excel_bytes, nome_arquivo
from src import ticker_service

st.set_page_config(page_title="Apuração LTI", layout="wide")
st.title("📊 Apuração LTI — TSR IBrX-50 TIM")
st.caption("Calcula TSR relativo ao IBrX-50 para as outorgas do programa LTI da TIM.")

# ---------------------------------------------------------------------------
# Bloco 1 — Configuração
# ---------------------------------------------------------------------------
st.subheader("1. Configuração")

col1, col2 = st.columns([1, 2])
with col1:
    opcoes = ["Todas"] + [str(a) for a in sorted(OUTORGAS.keys())]
    sel = st.selectbox("Outorga:", opcoes)
    anos_calcular = list(OUTORGAS.keys()) if sel == "Todas" else [int(sel)]

with col2:
    with st.expander("Parâmetros da outorga selecionada"):
        ano_preview = anos_calcular[0]
        cfg = OUTORGAS[ano_preview]
        st.markdown(f"""
        | Parâmetro | Valor |
        |-----------|-------|
        | P0 | {cfg.dt_p0_ini} → {cfg.dt_p0_fim} |
        | P Final | {cfg.dt_pf_ini} → {cfg.dt_pf_fim} |
        | Dividendos | {cfg.dt_divs_ini} → {cfg.dt_divs_fim} |
        | Exclusões forçadas | {', '.join(cfg.exclusoes_forcadas) or 'nenhuma'} |
        | Substituições | {cfg.substituicoes or 'nenhuma'} |
        | Tickers IBrX-50 | {len(cfg.tickers)} |
        """)
        st.text("Tickers: " + ", ".join(cfg.tickers))

# ---------------------------------------------------------------------------
# Bloco 2 — Execução
# ---------------------------------------------------------------------------
st.subheader("2. Execução")
btn = st.button("▶ Calcular Apuração", type="primary")

if btn:
    log_msgs: list[str] = []

    with st.spinner("Carregando base de empresas B3..."):
        df_empresas = ticker_service.carregar_empresas()
    if df_empresas.empty:
        st.error("Não foi possível carregar a base de empresas B3.")
        st.stop()

    resultados_session: dict = {}
    for ano in anos_calcular:
        cfg = OUTORGAS[ano]
        st.write(f"**Processando outorga {ano}** ({len(cfg.tickers)} tickers)...")
        prog = st.progress(0)
        total = len(cfg.tickers)

        ticker_idx = [0]

        def _logger_prog(msg: str) -> None:
            log_msgs.append(msg)
            if msg.strip().startswith("["):
                ticker_idx[0] += 1
                prog.progress(min(ticker_idx[0] / total, 1.0))

        resultado = calcular_outorga(cfg, df_empresas, logger=_logger_prog)
        resultados_session[ano] = resultado
        prog.progress(1.0)
        st.success(f"Outorga {ano}: {resultado.n_incluidos} incluídos | {resultado.n_excluidos} excluídos")

    st.session_state["lti_resultados"] = resultados_session
    with st.expander("Log de processamento"):
        st.text("\n".join(log_msgs))

# ---------------------------------------------------------------------------
# Bloco 3 — Resultados
# ---------------------------------------------------------------------------
if "lti_resultados" in st.session_state and st.session_state["lti_resultados"]:
    st.subheader("3. Resultados")
    resultados_session = st.session_state["lti_resultados"]

    for ano, resultado in sorted(resultados_session.items()):
        st.markdown(f"### Outorga {ano}")

        # Ranking table
        rows = []
        for t in resultado.ranking:
            rows.append({
                "Rank": t.rank,
                "Ticker": t.ticker,
                "Grupo": t.grupo,
                "VWAP P0": round(t.vwap_p0 or 0, 4),
                "VWAP Pf": round(t.vwap_pf or 0, 4),
                "Divs (R$)": round(t.dividendos_total, 4),
                "TSR (%)": round((t.tsr or 0) * 100, 4),
            })
        df_rank = pd.DataFrame(rows)

        def _color_tsr(val):
            if isinstance(val, (int, float)):
                color = "#1a7a1a" if val > 0 else "#b30000" if val < 0 else "inherit"
                return f"color: {color}; font-weight: bold"
            return ""

        def _highlight_tim(row):
            if row["Ticker"] == "TIMS3":
                return ["background-color: #FFFACD"] * len(row)
            return [""] * len(row)

        st.dataframe(
            df_rank.style.map(_color_tsr, subset=["TSR (%)"]).apply(_highlight_tim, axis=1),
            use_container_width=True,
        )

        # Group summary
        with st.expander("Grupos"):
            for g, members in sorted(resultado.grupos.items()):
                if not members:
                    continue
                tsrs = [m.tsr * 100 for m in members if m.tsr is not None]
                tim_aqui = any(m.ticker == "TIMS3" for m in members)
                label = (
                    f"**Grupo {g}** — ranks {members[0].rank}–{members[-1].rank} "
                    f"| TSR médio {sum(tsrs)/len(tsrs):+.2f}%" if tsrs else f"**Grupo {g}** — vazio"
                )
                if tim_aqui:
                    label += " ◄ **TIM**"
                st.markdown(label)

        # Exclusions and divergences
        excluidos = [t for t in resultado.tickers if t.status != "INCLUIDO"]
        divergencias = [t for t in resultado.tickers if t.divergencia_yf]
        if excluidos or divergencias:
            with st.expander(
                f"Exclusões e divergências ({len(excluidos)} excluídos, {len(divergencias)} divergências YF)"
            ):
                if excluidos:
                    exc_rows = [
                        {"Ticker": t.ticker, "Status": t.status, "Motivo": t.motivo_exclusao}
                        for t in excluidos
                    ]
                    st.dataframe(pd.DataFrame(exc_rows), use_container_width=True)
                if divergencias:
                    div_rows = [
                        {"Ticker": t.ticker, "Divergência": t.divergencia_yf}
                        for t in divergencias
                    ]
                    st.dataframe(pd.DataFrame(div_rows), use_container_width=True)

        # Download button
        xlsx_bytes = gerar_excel_bytes(resultado)
        st.download_button(
            label=f"📥 Baixar Excel Outorga {ano}",
            data=xlsx_bytes,
            file_name=nome_arquivo(resultado),
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        st.divider()
