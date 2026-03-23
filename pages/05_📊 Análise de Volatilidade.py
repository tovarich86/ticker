import streamlit as st
import pandas as pd
import numpy as np
import yfinance as yf
import plotly.graph_objects as go
import plotly.figure_factory as ff
from datetime import datetime, timedelta
import warnings
import sys, os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

st.set_page_config(page_title="Volatilidade", layout="wide")
st.title("📊 Análise de Volatilidade")
st.caption("Metodologias estatísticas com dados do Yahoo Finance")

# ---------------------------------------------------------------------------
# Verificação do pacote arch (GARCH)
# ---------------------------------------------------------------------------
try:
    from arch import arch_model
    GARCH_DISPONIVEL = True
except ImportError:
    GARCH_DISPONIVEL = False

FA = 252  # dias úteis para anualização

# ---------------------------------------------------------------------------
# Funções — Rolling (para o gráfico)
# ---------------------------------------------------------------------------

def roll_historica(df, janela):
    lr = np.log(df['Close'] / df['Close'].shift(1))
    return lr.rolling(janela).std() * np.sqrt(FA)

def roll_parkinson(df, janela):
    log_hl2 = np.log(df['High'] / df['Low']) ** 2
    return np.sqrt(log_hl2.rolling(janela).mean() / (4 * np.log(2)) * FA)

def roll_garman_klass(df, janela):
    log_hl2 = np.log(df['High'] / df['Low']) ** 2
    log_co2 = np.log(df['Close'] / df['Open']) ** 2
    gk = 0.5 * log_hl2 - (2 * np.log(2) - 1) * log_co2
    return np.sqrt(gk.rolling(janela).mean() * FA)

def roll_rogers_satchell(df, janela):
    rs = (np.log(df['High'] / df['Open']) * np.log(df['High'] / df['Close'])
        + np.log(df['Low']  / df['Open']) * np.log(df['Low']  / df['Close']))
    return np.sqrt(rs.rolling(janela).mean() * FA)

def roll_yang_zhang(df, janela, k=0.34):
    log_oc = np.log(df['Open'] / df['Close'].shift(1))
    log_co = np.log(df['Close'] / df['Open'])
    rs     = (np.log(df['High'] / df['Open']) * np.log(df['High'] / df['Close'])
            + np.log(df['Low']  / df['Open']) * np.log(df['Low']  / df['Close']))
    v_n = log_oc.rolling(janela).var()
    v_d = log_co.rolling(janela).var()
    rs_m = rs.rolling(janela).mean()
    k_adj = k / (1 + k + (janela + 1) / (janela - 1))
    return np.sqrt((v_n + k_adj * v_d + (1 - k_adj) * rs_m) * FA)

def roll_ewma(df, lam):
    """EWMA RiskMetrics: σ²_t = λ·σ²_{t-1} + (1-λ)·r²_t"""
    lr = np.log(df['Close'] / df['Close'].shift(1))
    var_ew = (lr ** 2).ewm(alpha=1 - lam, adjust=False).mean()
    return np.sqrt(var_ew * FA)

def roll_garch(df):
    """GARCH(1,1): volatilidade condicional ajustada em todo o período."""
    lr = np.log(df['Close'] / df['Close'].shift(1)).dropna() * 100
    if len(lr) < 100:
        return pd.Series(np.nan, index=df.index)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        res = arch_model(lr, vol='Garch', p=1, q=1, dist='normal', rescale=False).fit(disp='off')
    cond_vol = res.conditional_volatility / 100 * np.sqrt(FA)
    return cond_vol.reindex(df.index)

# ---------------------------------------------------------------------------
# Funções — Realizadas por subperíodo (para a tabela por ano)
# ---------------------------------------------------------------------------

# Cada função recebe um slice de df com exatamente N dias úteis e retorna
# a volatilidade realizada anualizada — pronta para uso em Black-Scholes / MC.

def vol_periodo_historica(df):
    lr = np.log(df['Close'] / df['Close'].shift(1)).dropna()
    return lr.std() * np.sqrt(FA)

def vol_periodo_parkinson(df):
    log_hl2 = (np.log(df['High'] / df['Low']) ** 2).dropna()
    return np.sqrt(log_hl2.mean() / (4 * np.log(2)) * FA)

def vol_periodo_garman_klass(df):
    log_hl2 = np.log(df['High'] / df['Low']) ** 2
    log_co2 = np.log(df['Close'] / df['Open']) ** 2
    gk = (0.5 * log_hl2 - (2 * np.log(2) - 1) * log_co2).dropna()
    return np.sqrt(gk.mean() * FA)

def vol_periodo_rogers_satchell(df):
    rs = (np.log(df['High'] / df['Open']) * np.log(df['High'] / df['Close'])
        + np.log(df['Low']  / df['Open']) * np.log(df['Low']  / df['Close'])).dropna()
    return np.sqrt(rs.mean() * FA)

def vol_periodo_yang_zhang(df, k=0.34):
    """YZ direto sobre o slice — inclui 1 dia extra para o gap overnight do 1º dia."""
    log_oc = np.log(df['Open'] / df['Close'].shift(1)).dropna()
    log_co = np.log(df['Close'] / df['Open']).dropna()
    log_ho = np.log(df['High'] / df['Open'])
    log_hc = np.log(df['High'] / df['Close'])
    log_lo = np.log(df['Low']  / df['Open'])
    log_lc = np.log(df['Low']  / df['Close'])
    rs = (log_ho * log_hc + log_lo * log_lc).dropna()
    n = len(rs)
    if n < 5:
        return np.nan
    k_adj = k / (1 + k + (n + 1) / (n - 1))
    yz = log_oc.var() + k_adj * log_co.var() + (1 - k_adj) * rs.mean()
    return np.sqrt(yz * FA)

def vol_periodo_ewma(df, lam):
    """EWMA sobre o slice — o valor final representa a vol corrente ponderada."""
    lr = np.log(df['Close'] / df['Close'].shift(1)).dropna()
    var_ew = (lr ** 2).ewm(alpha=1 - lam, adjust=False).mean()
    return float(np.sqrt(var_ew.iloc[-1] * FA))

def vol_periodo_garch(df):
    """GARCH(1,1) ajustado sobre o slice — retorna vol condicional final."""
    lr = np.log(df['Close'] / df['Close'].shift(1)).dropna() * 100
    if len(lr) < 60:
        return np.nan
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        res = arch_model(lr, vol='Garch', p=1, q=1, dist='normal', rescale=False).fit(disp='off')
    return float(res.conditional_volatility.iloc[-1] / 100 * np.sqrt(FA))

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _is_b3(t: str) -> bool:
    base = ''.join(c for c in t if not c.isdigit())
    num  = ''.join(c for c in t if c.isdigit())
    return len(base) == 4 and num in {'3', '4', '5', '6', '11'}

def fmt_pct(v):
    return f"{v * 100:.2f}%" if pd.notna(v) else "N/D"

OPCOES_MET = ["Histórica (C-C)", "Parkinson", "Garman-Klass",
              "Rogers-Satchell", "Yang-Zhang", "EWMA"]
if GARCH_DISPONIVEL:
    OPCOES_MET.append("GARCH(1,1)")

DESCRICOES = {
    "Histórica (C-C)":  "Desvio padrão dos log-retornos diários. Referência, mas ignora variação intraday.",
    "Parkinson":        "Usa High/Low. ~5× mais eficiente que C-C, mas ignora gaps e tendência.",
    "Garman-Klass":     "Usa OHLC. Mais preciso que Parkinson, mas assume ausência de gaps noturnos.",
    "Rogers-Satchell":  "Usa OHLC, independente de drift. Robusto para ativos em tendência.",
    "Yang-Zhang":       "Combina gap overnight + Rogers-Satchell. Estimador de menor variância disponível.",
    "EWMA":             "RiskMetrics (λ=0,94): pondera mais as observações recentes. Reage rápido a choques.",
    "GARCH(1,1)":       "Modelo paramétrico de volatilidade condicional. Captura clustering de volatilidade.",
}

# ---------------------------------------------------------------------------
# Inputs
# ---------------------------------------------------------------------------

col1, col2 = st.columns(2)
with col1:
    tickers_raw = st.text_input("Tickers:", placeholder="Ex: PETR4, VALE3, AAPL",
                                help="Tickers B3 sem .SA — sufixo adicionado automaticamente")
with col2:
    metodologias = st.multiselect("Metodologias:", OPCOES_MET,
                                  default=["Histórica (C-C)", "Garman-Klass", "Yang-Zhang", "EWMA"])

col3, col4, col5 = st.columns(3)
dt_hoje = datetime.now().date()
with col3:
    dt_ini = st.date_input("Data inicial:", value=dt_hoje - timedelta(days=365 * 3), format="DD/MM/YYYY")
with col4:
    dt_fim = st.date_input("Data final:", value=dt_hoje, format="DD/MM/YYYY")
with col5:
    janela_chart = st.selectbox("Janela rolling (gráfico):", [21, 42, 63, 126],
                                format_func=lambda x: {21:"21d (~1m)", 42:"42d (~2m)",
                                                        63:"63d (~3m)", 126:"126d (~6m)"}[x])

lam = 0.94
if "EWMA" in metodologias:
    lam = st.slider("Lambda EWMA (λ):", 0.85, 0.99, 0.94, 0.01,
                    help="Valores próximos de 1 dão mais peso ao passado. RiskMetrics usa 0,94.")

if not GARCH_DISPONIVEL and "GARCH(1,1)" in metodologias:
    st.warning("Pacote `arch` não instalado. Execute `pip install arch` para habilitar o GARCH.")

with st.expander("O que cada metodologia calcula?"):
    for met in OPCOES_MET:
        st.markdown(f"**{met}:** {DESCRICOES.get(met, '')}")

st.markdown("---")
btn = st.button("Calcular", type="primary")

# ---------------------------------------------------------------------------
# Processamento
# ---------------------------------------------------------------------------

if btn:
    if not tickers_raw.strip():
        st.warning("Informe pelo menos um ticker.")
        st.stop()
    if not metodologias:
        st.warning("Selecione ao menos uma metodologia.")
        st.stop()
    if dt_ini >= dt_fim:
        st.warning("A data inicial deve ser anterior à data final.")
        st.stop()

    tickers_list = [t.strip().upper() for t in tickers_raw.split(',') if t.strip()]
    yf_tickers   = [f"{t}.SA" if _is_b3(t) else t for t in tickers_list]
    ticker_map   = dict(zip(yf_tickers, tickers_list))

    # Baixa com margem extra para warmup do GARCH/EWMA
    dt_ini_dl = pd.Timestamp(dt_ini) - timedelta(days=janela_chart * 3)

    with st.spinner("Baixando dados..."):
        dados = yf.download(yf_tickers, start=dt_ini_dl, end=dt_fim + timedelta(days=1),
                            progress=False, auto_adjust=True)

    if dados.empty:
        st.error("Nenhum dado retornado. Verifique os tickers informados.")
        st.stop()

    if not isinstance(dados.columns, pd.MultiIndex):
        dados.columns = pd.MultiIndex.from_tuples([(c, yf_tickers[0]) for c in dados.columns])

    anos_range = list(range(pd.Timestamp(dt_ini).year, pd.Timestamp(dt_fim).year + 1))

    tab_anos, tab_rolling, tab_corr = st.tabs(
        ["📅 Volatilidade por Janela", "📈 Rolling", "🔗 Correlação"]
    )

    # -----------------------------------------------------------------------
    # TAB 1 — Volatilidade por Ano
    # -----------------------------------------------------------------------
    with tab_anos:
        st.subheader("Volatilidade Realizada por Janela (anualizada)")
        st.caption(
            "Cada linha usa os últimos N dias úteis contados a partir da data final. "
            "Uso direto como input de volatilidade em Black-Scholes, Binomial e Monte Carlo."
        )

        ROLL_FNS = {
            "Yang-Zhang": lambda df: roll_yang_zhang(df, janela_chart),
            "EWMA":        lambda df: roll_ewma(df, lam),
            "GARCH(1,1)":  lambda df: roll_garch(df) if GARCH_DISPONIVEL else pd.Series(np.nan, index=df.index),
        }

        for yf_t in yf_tickers:
            nome = ticker_map[yf_t]
            try:
                df_full = dados.xs(yf_t, axis=1, level=1).dropna(how='all')
            except KeyError:
                st.warning(f"Sem dados para {nome}.")
                continue

            # Slice apenas dentro do período selecionado pelo usuário
            df_periodo = df_full.loc[pd.Timestamp(dt_ini):pd.Timestamp(dt_fim)]
            n_dias_total = len(df_periodo)

            # Monta lista de janelas: 252, 504, 756 ... até o limite dos dados
            janelas = []
            i = 1
            while i * 252 <= n_dias_total:
                janelas.append((i * 252, f"{i} ano{'s' if i > 1 else ''} ({i * 252}d úteis)"))
                i += 1
            # Adiciona o período completo se não for múltiplo exato de 252
            if n_dias_total % 252 != 0 and n_dias_total > 0:
                janelas.append((n_dias_total, f"Período completo ({n_dias_total}d úteis)"))

            if not janelas:
                st.warning(f"Período insuficiente para {nome} (mínimo: 252 dias úteis).")
                continue

            FNS_PERIODO = {
                "Histórica (C-C)": vol_periodo_historica,
                "Parkinson":       vol_periodo_parkinson,
                "Garman-Klass":    vol_periodo_garman_klass,
                "Rogers-Satchell": vol_periodo_rogers_satchell,
                "Yang-Zhang":      vol_periodo_yang_zhang,
            }

            rows = []
            for n_dias_janela, label in janelas:
                # Últimos N dias úteis dentro do período
                df_slice = df_periodo.iloc[-n_dias_janela:]
                # Para YZ: inclui 1 dia extra anterior para calcular o gap overnight do 1º dia
                idx_extra = max(0, len(df_periodo) - n_dias_janela - 1)
                df_slice_yz = df_periodo.iloc[idx_extra:]

                row = {"Janela": label}
                for met in metodologias:
                    if met == "Yang-Zhang":
                        row[met] = fmt_pct(vol_periodo_yang_zhang(df_slice_yz))
                    elif met == "EWMA":
                        row[met] = fmt_pct(vol_periodo_ewma(df_slice, lam))
                    elif met == "GARCH(1,1)" and GARCH_DISPONIVEL:
                        row[met] = fmt_pct(vol_periodo_garch(df_slice))
                    elif met in FNS_PERIODO:
                        row[met] = fmt_pct(FNS_PERIODO[met](df_slice))
                rows.append(row)

            if rows:
                st.markdown(f"**{nome}**")
                df_tab = pd.DataFrame(rows).set_index("Janela")
                st.dataframe(df_tab, use_container_width=True)
            else:
                st.info(f"Sem dados suficientes para {nome}.")

    # -----------------------------------------------------------------------
    # TAB 2 — Rolling
    # -----------------------------------------------------------------------
    with tab_rolling:
        st.subheader(f"Volatilidade Rolling — janela {janela_chart}d (anualizada)")

        CHART_FNS = {
            "Histórica (C-C)": lambda df: roll_historica(df, janela_chart),
            "Parkinson":       lambda df: roll_parkinson(df, janela_chart),
            "Garman-Klass":    lambda df: roll_garman_klass(df, janela_chart),
            "Rogers-Satchell": lambda df: roll_rogers_satchell(df, janela_chart),
            "Yang-Zhang":      lambda df: roll_yang_zhang(df, janela_chart),
            "EWMA":            lambda df: roll_ewma(df, lam),
            "GARCH(1,1)":      lambda df: roll_garch(df) if GARCH_DISPONIVEL else pd.Series(np.nan, index=df.index),
        }

        dt_ini_ts = pd.Timestamp(dt_ini)
        dt_fim_ts = pd.Timestamp(dt_fim)

        for yf_t in yf_tickers:
            nome = ticker_map[yf_t]
            try:
                df_full = dados.xs(yf_t, axis=1, level=1).dropna(how='all')
            except KeyError:
                continue

            fig = go.Figure()
            for met in metodologias:
                serie = CHART_FNS[met](df_full)
                serie = serie[(serie.index >= dt_ini_ts) & (serie.index <= dt_fim_ts)].dropna() * 100
                if serie.empty:
                    continue
                fig.add_trace(go.Scatter(x=serie.index, y=serie.round(2), name=met, mode='lines'))

            fig.update_layout(
                title=nome,
                yaxis_title="Volatilidade Anualizada (%)",
                hovermode="x unified",
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
                height=400,
                margin=dict(t=60),
            )
            st.plotly_chart(fig, use_container_width=True)

    # -----------------------------------------------------------------------
    # TAB 3 — Correlação
    # -----------------------------------------------------------------------
    with tab_corr:
        if len(yf_tickers) < 2:
            st.info("Adicione ao menos 2 tickers para ver a correlação.")
        else:
            st.subheader("Correlação dos Log-Retornos no Período")
            try:
                closes = dados['Close'].copy()
                closes.columns = [ticker_map.get(c, c) for c in closes.columns]
                closes = closes[pd.Timestamp(dt_ini):pd.Timestamp(dt_fim)]
                log_rets = np.log(closes / closes.shift(1)).dropna()
                corr = log_rets.corr().round(2)

                fig_corr = ff.create_annotated_heatmap(
                    z=corr.values,
                    x=list(corr.columns),
                    y=list(corr.index),
                    colorscale='RdBu',
                    reversescale=True,
                    zmin=-1, zmax=1,
                    showscale=True,
                )
                fig_corr.update_layout(height=420)
                st.plotly_chart(fig_corr, use_container_width=True)
            except Exception as e:
                st.error(f"Erro ao calcular correlação: {e}")

    # -----------------------------------------------------------------------
    # EXPORTAÇÃO EXCEL — auditoria completa
    # -----------------------------------------------------------------------
    st.markdown("---")
    st.subheader("Exportar para Excel")

    from io import BytesIO

    def gerar_excel():
        buf = BytesIO()
        with pd.ExcelWriter(buf, engine='xlsxwriter') as writer:
            wb  = writer.book
            fmt_header  = wb.add_format({'bold': True, 'bg_color': '#1F3864', 'font_color': 'white', 'border': 1})
            fmt_pct_xl  = wb.add_format({'num_format': '0.00%', 'border': 1})
            fmt_num     = wb.add_format({'num_format': '0.000000', 'border': 1})
            fmt_num2    = wb.add_format({'num_format': '0.00', 'border': 1})
            fmt_date    = wb.add_format({'num_format': 'dd/mm/yyyy', 'border': 1})
            fmt_cell    = wb.add_format({'border': 1})

            # --- Aba Resumo (todas as janelas, todos os tickers) ---
            resumo_rows = []
            for yf_t in yf_tickers:
                nome = ticker_map[yf_t]
                try:
                    df_full = dados.xs(yf_t, axis=1, level=1).dropna(how='all')
                except KeyError:
                    continue
                df_periodo = df_full.loc[pd.Timestamp(dt_ini):pd.Timestamp(dt_fim)]
                n_dias_total = len(df_periodo)
                janelas = []
                i = 1
                while i * 252 <= n_dias_total:
                    janelas.append((i * 252, f"{i} ano{'s' if i > 1 else ''} ({i*252}d úteis)"))
                    i += 1
                if n_dias_total % 252 != 0 and n_dias_total > 0:
                    janelas.append((n_dias_total, f"Período completo ({n_dias_total}d úteis)"))

                FNS_P = {
                    "Histórica (C-C)": vol_periodo_historica,
                    "Parkinson":       vol_periodo_parkinson,
                    "Garman-Klass":    vol_periodo_garman_klass,
                    "Rogers-Satchell": vol_periodo_rogers_satchell,
                }
                for n_j, label in janelas:
                    df_slice = df_periodo.iloc[-n_j:]
                    idx_extra = max(0, len(df_periodo) - n_j - 1)
                    df_slice_yz = df_periodo.iloc[idx_extra:]
                    row = {"Ticker": nome, "Janela": label, "Dias Úteis": n_j}
                    for met in metodologias:
                        if met == "Yang-Zhang":
                            v = vol_periodo_yang_zhang(df_slice_yz)
                        elif met == "EWMA":
                            v = vol_periodo_ewma(df_slice, lam)
                        elif met == "GARCH(1,1)" and GARCH_DISPONIVEL:
                            v = vol_periodo_garch(df_slice)
                        elif met in FNS_P:
                            v = FNS_P[met](df_slice)
                        else:
                            v = np.nan
                        row[met] = v
                    resumo_rows.append(row)

            if resumo_rows:
                df_resumo = pd.DataFrame(resumo_rows)
                df_resumo.to_excel(writer, sheet_name='Resumo', index=False)
                ws = writer.sheets['Resumo']
                ws.set_column('A:B', 18)
                ws.set_column('C:C', 12)
                for col_idx in range(3, len(df_resumo.columns)):
                    ws.set_column(col_idx, col_idx, 18, fmt_pct_xl)

            # --- Uma aba por ticker com cotações + cálculos intermediários ---
            for yf_t in yf_tickers:
                nome = ticker_map[yf_t]
                try:
                    df_full = dados.xs(yf_t, axis=1, level=1).dropna(how='all')
                except KeyError:
                    continue
                df_periodo = df_full.loc[pd.Timestamp(dt_ini):pd.Timestamp(dt_fim)]
                if df_periodo.empty:
                    continue

                d = df_periodo.copy()
                d.index = d.index.tz_localize(None)  # remove timezone para Excel

                audit = pd.DataFrame(index=d.index)
                audit.index.name = 'Data'
                audit['Open']   = d['Open']
                audit['High']   = d['High']
                audit['Low']    = d['Low']
                audit['Close']  = d['Close']
                if 'Volume' in d.columns:
                    audit['Volume'] = d['Volume']

                # Retorno logarítmico
                lr = np.log(d['Close'] / d['Close'].shift(1))
                audit['ln(C/Cprev)']   = lr
                audit['ln(C/Cprev)²']  = lr ** 2

                # Parkinson
                audit['ln(H/L)']       = np.log(d['High'] / d['Low'])
                audit['ln(H/L)²']      = audit['ln(H/L)'] ** 2
                audit['Parkinson_term'] = audit['ln(H/L)²'] / (4 * np.log(2))

                # Garman-Klass
                audit['ln(C/O)²']      = np.log(d['Close'] / d['Open']) ** 2
                audit['GK_term']       = 0.5 * audit['ln(H/L)²'] - (2 * np.log(2) - 1) * audit['ln(C/O)²']

                # Rogers-Satchell
                audit['ln(H/O)']       = np.log(d['High'] / d['Open'])
                audit['ln(H/C)']       = np.log(d['High'] / d['Close'])
                audit['ln(L/O)']       = np.log(d['Low']  / d['Open'])
                audit['ln(L/C)']       = np.log(d['Low']  / d['Close'])
                audit['RS_term']       = audit['ln(H/O)'] * audit['ln(H/C)'] + audit['ln(L/O)'] * audit['ln(L/C)']

                # Yang-Zhang
                audit['ln(O/Cprev)']   = np.log(d['Open'] / d['Close'].shift(1))  # overnight gap
                audit['ln(C/O)']       = np.log(d['Close'] / d['Open'])            # intraday

                # EWMA
                ewma_var = (lr ** 2).ewm(alpha=1 - lam, adjust=False).mean()
                audit['EWMA_var']      = ewma_var
                audit['EWMA_vol_%aa']  = np.sqrt(ewma_var * FA) * 100

                sheet_name = nome[:31]  # Excel limita a 31 chars
                audit.reset_index().to_excel(writer, sheet_name=sheet_name, index=False)
                ws = writer.sheets[sheet_name]
                ws.set_column('A:A', 12)
                ws.set_column('B:F', 14, fmt_num2)
                ws.set_column('G:Z', 14, fmt_num)

        buf.seek(0)
        return buf

    if st.button("Gerar Excel para Auditoria"):
        with st.spinner("Gerando arquivo..."):
            excel_buf = gerar_excel()
        nome_arquivo = f"volatilidade_{'_'.join(tickers_list)}_{dt_fim.strftime('%Y%m%d')}.xlsx"
        st.download_button(
            label="Baixar Excel",
            data=excel_buf,
            file_name=nome_arquivo,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
