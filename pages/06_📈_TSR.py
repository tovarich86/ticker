import streamlit as st
import pandas as pd
import numpy as np
import requests
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from io import BytesIO
import sys, os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src import ticker_service, b3_engine

st.set_page_config(page_title="TSR", layout="wide")
st.title("📈 TSR — Total Shareholder Return")
st.caption("B3 (COTAHIST + Proventos + Eventos Corporativos) | Internacional (Yahoo Finance)")

# ---------------------------------------------------------------------------
# Helpers gerais
# ---------------------------------------------------------------------------

def _parece_b3(ticker: str) -> bool:
    """Retorna True se o ticker tiver padrão B3: 4 letras + número (3,4,5,6,11)."""
    base = ''.join(c for c in ticker if not c.isdigit())
    num  = ''.join(c for c in ticker if c.isdigit())
    return len(base) == 4 and num in {'3', '4', '5', '6', '11'}

def _yf_symbol(ticker: str) -> str:
    """Converte ticker para símbolo Yahoo Finance: VALE3 → VALE3.SA, AAPL → AAPL."""
    return f"{ticker}.SA" if _parece_b3(ticker) else ticker

# ---------------------------------------------------------------------------
# Helpers de preço
# ---------------------------------------------------------------------------

def _buscar_cotacoes_periodo(tickers: list, dt_ini, dt_fim) -> pd.DataFrame:
    """Baixa COTAHIST para o período e retorna DataFrame com Open/High/Low/Close/Average/Volume."""
    dias = b3_engine.listar_dias_uteis(dt_ini, dt_fim)
    frames = []
    with requests.Session() as session:
        with ThreadPoolExecutor(max_workers=5) as ex:
            futs = [ex.submit(b3_engine.baixar_e_parsear_dia, d, tickers, session) for d in dias]
            for f in futs:
                r = f.result()
                if r is not None:
                    frames.append(r)
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    df['Date'] = pd.to_datetime(df['Date'])
    return df.sort_values(['Ticker', 'Date'])


def _calcular_preco(df_ticker: pd.DataFrame, tipo: str) -> float | None:
    """Retorna o preço representativo do período conforme o tipo escolhido."""
    if df_ticker.empty:
        return None
    if tipo == "Fechamento (último dia)":
        return float(df_ticker.iloc[-1]['Close'])
    elif tipo == "Média Simples (closes)":
        return float(df_ticker['Close'].mean())
    elif tipo == "VWAP (média ponderada pelo volume)":
        # Usa Quantity (QUANTIDADE_NEGOCIADA do COTAHIST) diretamente
        qty = df_ticker['Quantity'] if 'Quantity' in df_ticker.columns else df_ticker['Volume'] / df_ticker['Average'].replace(0, np.nan)
        avg = df_ticker['Average']
        denom = qty.replace(0, np.nan).sum()
        if pd.isna(denom) or denom == 0:
            return float(avg.mean())
        return float((qty * avg).sum() / denom)
    return None

# ---------------------------------------------------------------------------
# Helpers Yahoo Finance (tickers internacionais)
# ---------------------------------------------------------------------------

def _buscar_cotacoes_yf(ticker: str, dt_ini, dt_fim) -> pd.DataFrame:
    """Cotações via Yahoo Finance. Adiciona .SA automaticamente para tickers B3."""
    try:
        df = yf.download(_yf_symbol(ticker), start=dt_ini, end=dt_fim + timedelta(days=1),
                         auto_adjust=False, progress=False)
        if df.empty:
            return pd.DataFrame()
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df = df.reset_index()
        df['Ticker'] = ticker
        df['Date'] = pd.to_datetime(df['Date']).dt.tz_localize(None)
        # Average como HLC/3; Quantity = Volume (ações negociadas)
        df['Average'] = (df['High'] + df['Low'] + df['Close']) / 3
        df['Quantity'] = df['Volume'].astype('Int64')
        return df[['Ticker', 'Date', 'Open', 'High', 'Low', 'Close', 'Average', 'Volume', 'Quantity']].sort_values('Date')
    except Exception:
        return pd.DataFrame()


def _buscar_dividendos_yf(ticker: str, t0: pd.Timestamp, t1: pd.Timestamp) -> pd.DataFrame:
    """Dividendos via Yahoo Finance no período [t0, t1]. Adiciona .SA para tickers B3."""
    try:
        divs = yf.Ticker(_yf_symbol(ticker)).dividends
        if divs.empty:
            return pd.DataFrame()
        divs = divs.reset_index()
        divs.columns = ['Date', 'value']
        divs['Date'] = pd.to_datetime(divs['Date']).dt.tz_localize(None)
        divs = divs[(divs['Date'] >= t0) & (divs['Date'] <= t1)].copy()
        if divs.empty:
            return pd.DataFrame()
        divs['Ticker'] = ticker
        divs['lastDatePriorEx'] = divs['Date'].dt.strftime('%d/%m/%Y')
        divs['paymentDate'] = ''
        divs['label'] = 'Dividendo'
        divs['typeStock'] = ''
        return divs[['Ticker', 'lastDatePriorEx', 'paymentDate', 'label', 'value']]
    except Exception:
        return pd.DataFrame()


def _buscar_splits_yf(ticker: str, t0: pd.Timestamp, t1: pd.Timestamp) -> pd.DataFrame:
    """Splits/reverse splits via Yahoo Finance no período [t0, t1]. Adiciona .SA para tickers B3.
    O campo 'factor' é o ratio direto (ex: 2.0 para split 2:1).
    Label 'SPLIT_YF' sinaliza que mult = factor (não aplica fórmula B3).
    """
    try:
        splits = yf.Ticker(_yf_symbol(ticker)).splits
        if splits.empty:
            return pd.DataFrame()
        splits = splits.reset_index()
        splits.columns = ['Date', 'factor']
        splits['Date'] = pd.to_datetime(splits['Date']).dt.tz_localize(None)
        splits = splits[(splits['Date'] >= t0) & (splits['Date'] <= t1)].copy()
        if splits.empty:
            return pd.DataFrame()
        splits['Ticker'] = ticker
        splits['lastDatePrior'] = splits['Date'].dt.strftime('%d/%m/%Y')
        splits['label'] = 'SPLIT_YF'
        return splits[['Ticker', 'lastDatePrior', 'label', 'factor']]
    except Exception:
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# TSR
# ---------------------------------------------------------------------------

def _parse_float(val):
    try:
        return float(str(val).replace(',', '.'))
    except Exception:
        return np.nan


def calcular_tsr(ticker: str, p0: float, p_final: float,
                 df_divs: pd.DataFrame, df_bonif: pd.DataFrame,
                 t0: pd.Timestamp, t1: pd.Timestamp) -> dict:
    """
    TSR na base de 1 ação adquirida ao preço P0 em t0.

    Eventos corporativos (bonificações/splits/grupamentos) são tratados como
    multiplicadores da quantidade de ações:
        mult = ∏ (1 + factor_i)   para todos os eventos entre t0 e t1

    Dividendos são creditados proporcionalmente à quantidade de ações vigente
    na data ex (mais ações após splits = mais dividendos totais recebidos).

    TSR = (P_final × mult_final  −  P0  +  Σ div_j × mult_em_j) / P0
    """

    # --- Eventos corporativos ordenados ---
    eventos = []
    if not df_bonif.empty and 'lastDatePrior' in df_bonif.columns:
        for _, row in df_bonif.iterrows():
            dt    = pd.to_datetime(row.get('lastDatePrior', ''), format='%d/%m/%Y', errors='coerce')
            fac   = _parse_float(row.get('factor', 0))
            label = str(row.get('label', '')).upper()
            if pd.notna(dt) and pd.notna(fac) and fac != 0:
                if t0 < dt <= t1:
                    # Multiplicador por fonte:
                    # B3: factor = "novas ações por 100 existentes" → mult = 1 + factor/100
                    #   BONIFICACAO 1%    factor=1    → 1.01
                    #   DESDOBRAMENTO 5:1 factor=400  → 5.00
                    #   GRUPAMENTO 5:1    factor=-80  → 0.20
                    # Yahoo (SPLIT_YF): factor já é o ratio direto → mult = factor
                    #   Split 2:1  factor=2.0  → 2.00
                    #   RSplit 1:10 factor=0.1 → 0.10
                    if label == 'SPLIT_YF':
                        mult = fac
                    else:
                        mult = 1.0 + fac / 100.0
                    eventos.append({'date': dt, 'mult': round(mult, 8), 'factor': fac, 'label': row.get('label', '')})
    eventos.sort(key=lambda x: x['date'])

    # Multiplicador acumulado em cada ponto do tempo
    def mult_ate(data: pd.Timestamp) -> float:
        m = 1.0
        for ev in eventos:
            if ev['date'] <= data:
                m *= ev['mult']
        return m

    mult_final = mult_ate(t1)

    # --- Dividendos ---
    total_divs = 0.0
    divs_detail = []
    if not df_divs.empty and 'value' in df_divs.columns:
        for _, row in df_divs.iterrows():
            dt_ex = pd.to_datetime(row.get('lastDatePriorEx', ''), format='%d/%m/%Y', errors='coerce')
            val   = _parse_float(row.get('value', 0))
            if pd.isna(dt_ex) or pd.isna(val):
                continue
            # Quantidade de ações no momento do dividendo
            m_div = mult_ate(dt_ex)
            div_total = m_div * val
            total_divs += div_total
            divs_detail.append({
                'Data Ex':          row.get('lastDatePriorEx', ''),
                'Pagamento':        row.get('paymentDate', ''),
                'Tipo':             row.get('label', ''),
                'Valor/Ação (R$)':  val,
                'Multiplicador':    round(m_div, 6),
                'Total Recebido (R$)': round(div_total, 6),
            })

    # --- TSR decomposição ---
    p_final_adj  = p_final * mult_final
    ret_preco    = (p_final_adj - p0) / p0
    ret_divs     = total_divs / p0
    ret_bonif    = (mult_final - 1) * p_final / p0   # parcela de preço devida aos eventos
    tsr_total    = ret_preco + ret_divs

    return {
        'Ticker':             ticker,
        'P0 (R$)':            round(p0, 4),
        'P Final (R$)':       round(p_final, 4),
        'Mult. Corporativo':  round(mult_final, 6),
        'P Final Ajustado (R$)': round(p_final_adj, 4),
        'Dividendos/JCP (R$)':   round(total_divs, 4),
        'Ret. Preço (%)':     round(ret_preco * 100, 2),
        'Ret. Dividendos (%)': round(ret_divs * 100, 2),
        'TSR Total (%)':      round(tsr_total * 100, 2),
        '_divs_detail':       divs_detail,
        '_eventos':           eventos,
    }

# ---------------------------------------------------------------------------
# Inputs
# ---------------------------------------------------------------------------

st.subheader("1. Ativos")
tickers_raw = st.text_input("Tickers (B3 e/ou internacionais):",
                             placeholder="Ex: PETR4, VALE3, AAPL, MSFT",
                             help="Ações B3 usam COTAHIST; internacionais usam Yahoo Finance")

# Parseia tickers em tempo real para exibir inputs por ticker no modo manual
_tickers_preview = [t.strip().upper() for t in tickers_raw.split(',') if t.strip()]

st.subheader("2. Preço Inicial")
modo_p0 = st.radio("Origem do preço inicial:", ["Calcular por período", "Inserir valor manualmente"],
                   horizontal=True)

col_ini1, col_ini2, col_ini3 = st.columns(3)
dt_hoje = datetime.now().date()

if modo_p0 == "Calcular por período":
    with col_ini1:
        dt_p0_ini = st.date_input("Início período inicial:", value=dt_hoje - timedelta(days=365),
                                   format="DD/MM/YYYY", key="dt_p0_ini")
    with col_ini2:
        dt_p0_fim = st.date_input("Fim período inicial:", value=dt_hoje - timedelta(days=345),
                                   format="DD/MM/YYYY", key="dt_p0_fim")
    with col_ini3:
        tipo_p0 = st.selectbox("Tipo de preço inicial:",
                               ["Fechamento (último dia)", "Média Simples (closes)", "VWAP (média ponderada pelo volume)"])
    p0_por_ticker = {}
else:
    with col_ini1:
        dt_p0_fim = st.date_input("Data de referência:", value=dt_hoje - timedelta(days=365),
                                   format="DD/MM/YYYY", key="dt_p0_ref")
        dt_p0_ini = dt_p0_fim
    tipo_p0 = None
    # Um input por ticker
    p0_por_ticker = {}
    if _tickers_preview:
        cols_p0 = st.columns(min(len(_tickers_preview), 4))
        for i, t in enumerate(_tickers_preview):
            with cols_p0[i % 4]:
                p0_por_ticker[t] = st.number_input(f"P0 {t}:", min_value=0.0001,
                                                    value=10.0, step=0.01, format="%.4f",
                                                    key=f"p0_{t}")
    else:
        st.info("Digite os tickers acima para inserir os preços iniciais.")

st.subheader("3. Preço Final")
col_fim1, col_fim2, col_fim3 = st.columns(3)
with col_fim1:
    dt_pf_ini = st.date_input("Início período final:", value=dt_hoje - timedelta(days=20),
                               format="DD/MM/YYYY", key="dt_pf_ini")
with col_fim2:
    dt_pf_fim = st.date_input("Fim período final:", value=dt_hoje,
                               format="DD/MM/YYYY", key="dt_pf_fim")
with col_fim3:
    tipo_pf = st.selectbox("Tipo de preço final:",
                           ["Fechamento (último dia)", "Média Simples (closes)", "VWAP (média ponderada pelo volume)"])

st.markdown("---")
btn = st.button("Calcular TSR", type="primary")

# ---------------------------------------------------------------------------
# Processamento — salva resultados no session_state para sobreviver ao rerun
# ---------------------------------------------------------------------------

if btn:
    if not tickers_raw.strip():
        st.warning("Informe pelo menos um ticker.")
        st.stop()

    tickers = [t.strip().upper() for t in tickers_raw.split(',') if t.strip()]

    t0 = pd.Timestamp(dt_p0_fim)   # data de referência do preço inicial
    t1 = pd.Timestamp(dt_pf_fim)   # data de referência do preço final

    if t0 >= t1:
        st.warning("A data final deve ser posterior à data inicial.")
        st.stop()

    # Carrega empresas B3 apenas se houver tickers B3 na lista
    with st.spinner("Identificando tickers..."):
        df_empresas = ticker_service.carregar_empresas()

    tickers_b3  = [t for t in tickers if ticker_service.is_b3_ticker(t, df_empresas)]
    tickers_yf  = [t for t in tickers if not ticker_service.is_b3_ticker(t, df_empresas)]

    if tickers_b3 and df_empresas.empty:
        st.error("Não foi possível carregar a base de empresas da B3.")
        st.stop()

    resultados = []
    log_container = st.expander("Log de processamento", expanded=False)

    for ticker in tickers:
        is_b3 = ticker in tickers_b3
        moeda = "R$" if is_b3 else "$"

        with log_container:
            fonte = "B3" if is_b3 else "Yahoo Finance"
            st.write(f"**── {ticker} ({fonte}) ──**")

        # ── Preço Inicial ──────────────────────────────────────────────────
        p0_manual = p0_por_ticker.get(ticker)  # None se modo "Calcular por período"
        if p0_manual:
            p0 = float(p0_manual)
            df_ini_t = pd.DataFrame()
            with log_container:
                st.write(f"P0 manual: {moeda} {p0:.4f}")
        else:
            with log_container:
                st.write(f"Baixando cotações iniciais ({dt_p0_ini} → {dt_p0_fim})...")
            if is_b3:
                df_ini = _buscar_cotacoes_periodo([ticker], dt_p0_ini, dt_p0_fim)
                df_ini_t = df_ini[df_ini['Ticker'] == ticker] if not df_ini.empty else pd.DataFrame()
            else:
                df_ini_t = _buscar_cotacoes_yf(ticker, dt_p0_ini, dt_p0_fim)
            p0 = _calcular_preco(df_ini_t, tipo_p0)
            if p0 is None:
                with log_container:
                    st.warning(f"Sem cotações no período inicial para {ticker}. Pulando.")
                continue
            with log_container:
                st.write(f"P0 ({tipo_p0}): {moeda} {p0:.4f}  ({len(df_ini_t)} pregões)")

        # ── Preço Final ────────────────────────────────────────────────────
        with log_container:
            st.write(f"Baixando cotações finais ({dt_pf_ini} → {dt_pf_fim})...")
        if is_b3:
            df_fim = _buscar_cotacoes_periodo([ticker], dt_pf_ini, dt_pf_fim)
            df_fim_t = df_fim[df_fim['Ticker'] == ticker] if not df_fim.empty else pd.DataFrame()
        else:
            df_fim_t = _buscar_cotacoes_yf(ticker, dt_pf_ini, dt_pf_fim)
        p_final = _calcular_preco(df_fim_t, tipo_pf)
        if p_final is None:
            with log_container:
                st.warning(f"Sem cotações no período final para {ticker}. Pulando.")
            continue
        with log_container:
            st.write(f"P Final ({tipo_pf}): {moeda} {p_final:.4f}  ({len(df_fim_t)} pregões)")

        # ── Dividendos / JCP ───────────────────────────────────────────────
        with log_container:
            st.write(f"Buscando dividendos ({t0.date()} → {t1.date()})...")
        if is_b3:
            df_divs = ticker_service.buscar_dividendos_b3(ticker, df_empresas, t0, t1)
        else:
            df_divs = _buscar_dividendos_yf(ticker, t0, t1)
        n_divs = len(df_divs) if not df_divs.empty else 0
        with log_container:
            st.write(f"Dividendos encontrados: {n_divs} eventos")

        # ── Eventos Corporativos ───────────────────────────────────────────
        with log_container:
            st.write(f"Buscando eventos corporativos ({t0.date()} → {t1.date()})...")
        if is_b3:
            df_bonif = ticker_service.buscar_bonificacoes_b3(ticker, df_empresas, t0, t1)
        else:
            df_bonif = _buscar_splits_yf(ticker, t0, t1)
        n_bonif = len(df_bonif) if not df_bonif.empty else 0
        with log_container:
            st.write(f"Eventos corporativos: {n_bonif}")

        # ── Cálculo TSR ────────────────────────────────────────────────────
        res = calcular_tsr(ticker, p0, p_final, df_divs, df_bonif, t0, t1)
        res['_df_cotacoes_ini'] = df_ini_t if p0_manual is None else pd.DataFrame()
        res['_df_cotacoes_fim'] = df_fim_t
        res['_df_divs']         = df_divs
        res['_df_bonif']        = df_bonif
        resultados.append(res)

    if not resultados:
        st.error("Nenhum resultado calculado. Verifique os tickers e as datas.")
        st.stop()

    cols_rank = ['Ticker', 'P0 (R$)', 'P Final (R$)', 'Mult. Corporativo',
                 'P Final Ajustado (R$)', 'Dividendos/JCP (R$)',
                 'Ret. Preço (%)', 'Ret. Dividendos (%)', 'TSR Total (%)']

    df_rank = (pd.DataFrame([{c: r[c] for c in cols_rank} for r in resultados])
               .sort_values('TSR Total (%)', ascending=False)
               .reset_index(drop=True))
    df_rank.index += 1
    df_rank.index.name = 'Pos.'

    # Gera Excel imediatamente após o cálculo
    def _gerar_excel(res_list, rank):
        buf = BytesIO()
        with pd.ExcelWriter(buf, engine='xlsxwriter') as writer:
            rank.to_excel(writer, sheet_name='Ranking')
            writer.sheets['Ranking'].set_column('B:J', 22)
            for res in res_list:
                t = res['Ticker']
                if not res['_df_cotacoes_ini'].empty:
                    df_ci = res['_df_cotacoes_ini'].copy()
                    df_ci['Date'] = df_ci['Date'].dt.strftime('%d/%m/%Y')
                    df_ci.to_excel(writer, sheet_name=f'{t}_P0', index=False)
                    writer.sheets[f'{t}_P0'].set_column('A:I', 16)
                if not res['_df_cotacoes_fim'].empty:
                    df_cf = res['_df_cotacoes_fim'].copy()
                    df_cf['Date'] = df_cf['Date'].dt.strftime('%d/%m/%Y')
                    df_cf.to_excel(writer, sheet_name=f'{t}_PFinal', index=False)
                    writer.sheets[f'{t}_PFinal'].set_column('A:I', 16)
                if not res['_df_divs'].empty:
                    res['_df_divs'].to_excel(writer, sheet_name=f'{t}_Divs', index=False)
                    writer.sheets[f'{t}_Divs'].set_column('A:J', 18)
                if res['_df_bonif'] is not None and not res['_df_bonif'].empty:
                    res['_df_bonif'].to_excel(writer, sheet_name=f'{t}_Eventos', index=False)
                    writer.sheets[f'{t}_Eventos'].set_column('A:J', 18)
                if res['_divs_detail']:
                    pd.DataFrame(res['_divs_detail']).to_excel(
                        writer, sheet_name=f'{t}_DivsAdj', index=False)
                    writer.sheets[f'{t}_DivsAdj'].set_column('A:F', 22)
        buf.seek(0)
        return buf

    # Persiste no session_state para sobreviver ao rerun do download_button
    st.session_state['tsr_resultados'] = resultados
    st.session_state['tsr_df_rank']    = df_rank
    st.session_state['tsr_excel']      = _gerar_excel(resultados, df_rank).getvalue()
    st.session_state['tsr_nomes']      = '_'.join(tickers)
    st.session_state['tsr_dt_fim']     = dt_pf_fim.strftime('%Y%m%d')

# ---------------------------------------------------------------------------
# Exibição — lê do session_state (persiste após rerun do download_button)
# ---------------------------------------------------------------------------

if 'tsr_resultados' in st.session_state and st.session_state['tsr_resultados']:
    resultados = st.session_state['tsr_resultados']
    df_rank    = st.session_state['tsr_df_rank']

    def _color_tsr(val):
        if isinstance(val, (int, float)):
            color = '#1a7a1a' if val > 0 else '#b30000' if val < 0 else 'inherit'
            return f'color: {color}; font-weight: bold'
        return ''

    st.subheader("Ranking TSR")
    st.dataframe(
        df_rank.style.applymap(_color_tsr, subset=['TSR Total (%)', 'Ret. Preço (%)', 'Ret. Dividendos (%)']),
        use_container_width=True
    )

    st.subheader("Detalhes por Ativo")
    for res in sorted(resultados, key=lambda x: x['TSR Total (%)'], reverse=True):
        ticker = res['Ticker']
        tsr    = res['TSR Total (%)']
        icon   = "🟢" if tsr > 0 else "🔴" if tsr < 0 else "⚪"
        with st.expander(f"{icon} {ticker}  |  TSR: {tsr:+.2f}%"):
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Preço Inicial (P0)", f"R$ {res['P0 (R$)']:.4f}")
            c2.metric("Preço Final Ajustado", f"R$ {res['P Final Ajustado (R$)']:.4f}",
                      delta=f"{res['Ret. Preço (%)']:+.2f}%")
            c3.metric("Dividendos/JCP", f"R$ {res['Dividendos/JCP (R$)']:.4f}",
                      delta=f"{res['Ret. Dividendos (%)']:+.2f}%")
            c4.metric("TSR Total", f"{tsr:+.2f}%")

            if res['_eventos']:
                st.markdown("**Eventos Corporativos no Período:**")
                df_ev = pd.DataFrame(res['_eventos'])
                df_ev['date'] = df_ev['date'].dt.strftime('%d/%m/%Y')
                df_ev = df_ev[['date', 'label', 'factor', 'mult']].rename(columns={
                    'date': 'Data Ex', 'label': 'Tipo', 'factor': 'Fator (API)', 'mult': 'Multiplicador'
                })
                st.dataframe(df_ev, use_container_width=True, hide_index=True)
            else:
                st.info("Nenhum evento corporativo no período.")

            if res['_divs_detail']:
                st.markdown("**Dividendos/JCP no Período:**")
                st.dataframe(pd.DataFrame(res['_divs_detail']), use_container_width=True, hide_index=True)
            else:
                st.info("Nenhum dividendo/JCP no período.")

    st.markdown("---")
    st.download_button(
        label="📥 Baixar Auditoria Excel",
        data=st.session_state['tsr_excel'],
        file_name=f"TSR_{st.session_state['tsr_nomes']}_{st.session_state['tsr_dt_fim']}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
