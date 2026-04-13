# src/lti/engine.py
from __future__ import annotations

import math
import json
import time
from base64 import b64encode
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Callable

import numpy as np
import pandas as pd
import requests
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor
from curl_cffi import requests as curl_requests

from src import b3_engine, ticker_service
from src.lti.config import OutorgaConfig, OUTORGAS

# ---------------------------------------------------------------------------
# Result dataclasses
# ---------------------------------------------------------------------------

@dataclass
class TickerResult:
    ticker: str                          # ticker efetivo usado no cálculo
    ticker_original: str                 # ticker da lista IBrX-50
    vwap_p0: float | None
    vwap_pf: float | None
    dividendos_total: float
    eventos_corporativos: list[dict] = field(default_factory=list)
    divs_ajustados: list[dict] = field(default_factory=list)
    tsr: float | None = None             # TSR como decimal (ex: 0.55 = 55%)
    mult_corporativo: float | None = None  # produto de todos os multiplicadores de eventos
    rank: int | None = None
    grupo: int | None = None
    status: str = "INCLUIDO"             # INCLUIDO | EXCLUIDO_FORCADO | SEM_DADOS
    motivo_exclusao: str = ""
    divergencia_yf: str | None = None
    df_cotacoes_p0: pd.DataFrame = field(default_factory=pd.DataFrame)
    df_cotacoes_pf: pd.DataFrame = field(default_factory=pd.DataFrame)
    df_dividendos: pd.DataFrame = field(default_factory=pd.DataFrame)
    df_bonificacoes: pd.DataFrame = field(default_factory=pd.DataFrame)


@dataclass
class ApuracaoResult:
    outorga: OutorgaConfig
    tickers: list[TickerResult]
    ranking: list[TickerResult] = field(default_factory=list)
    grupos: dict[int, list[TickerResult]] = field(default_factory=dict)
    n_incluidos: int = 0
    n_excluidos: int = 0
    timestamp: datetime = field(default_factory=datetime.now)


# ---------------------------------------------------------------------------
# TSR core (extracted and adapted from pages/06_📈_TSR.py)
# ---------------------------------------------------------------------------

def _parse_float(val) -> float:
    """
    Converte valor numérico retornado pela API B3 para float.
    A B3 retorna fatores como strings no formato brasileiro:
      vírgula = separador decimal, ponto = separador de milhar.
    Exemplos: "9.900,00000000000" → 9900.0 | "0,02500000000" → 0.025 | "100" → 100.0
    """
    try:
        s = str(val).strip()
        if "," in s:
            # Formato brasileiro: remove separador de milhar (ponto) e normaliza decimal
            s = s.replace(".", "").replace(",", ".")
        return float(s)
    except Exception:
        return math.nan


def _calcular_vwap(df: pd.DataFrame) -> float | None:
    """VWAP = Σ(Average × Quantity) / Σ(Quantity). Fallback: média simples."""
    if df.empty:
        return None
    qty = df["Quantity"].replace(0, np.nan)
    avg = df["Average"]
    denom = qty.sum()
    if pd.isna(denom) or denom == 0:
        return float(avg.mean())
    return float((qty * avg).sum() / denom)



def calcular_tsr(
    ticker: str,
    p0: float,
    p_final: float,
    df_divs: pd.DataFrame,
    df_bonif: pd.DataFrame,
    t0: pd.Timestamp,
    t1: pd.Timestamp,
) -> dict:
    """
    TSR para 1 ação adquirida ao preço P0 em t0.

    Eventos corporativos (bonificações/splits/grupamentos) são tratados como
    multiplicadores da quantidade de ações:
        mult = ∏ (1 + factor_i/100)  para label != 'SPLIT_YF'
        mult = factor_i              para label == 'SPLIT_YF' (Yahoo ratio direto)

    TSR = (P_final × mult_final − P0 + Σ div_j × mult_em_j) / P0
    """
    # --- Eventos corporativos ordenados por data ---
    # Regras de multiplicador por tipo de evento:
    #   BONIFICACAO / DESDOBRAMENTO: factor é percentual → mult = 1 + factor/100
    #     ex: BBAS3 DESDOBRAMENTO factor=100 → mult=2.0 (2:1 split)
    #         VIVT3 DESDOBRAMENTO factor=7900 → mult=80 (80:1 desdobramento)
    #         TIMS3 DESDOBRAMENTO factor=9900 → mult=100 (100:1 desdobramento)
    #     Nota: B3 retorna fatores em formato brasileiro ("7.900,00" = 7900); _parse_float
    #           trata o separador de milhar antes de converter.
    #   GRUPAMENTO: factor é ratio direto → mult = factor
    #     ex: VIVT3 GRUPAMENTO factor=0.025 → mult=0.025 (40:1 grupamento)
    #         TIMS3 GRUPAMENTO factor=0.01  → mult=0.01  (100:1 grupamento)
    #         MGLU3 GRUPAMENTO factor=0.10  → mult=0.10  (10:1 grupamento)
    #   RESG TOTAL RV: resgate de instrumento — sem efeito na quantidade de ações → ignorar
    #   SPLIT_YF: ratio direto (fonte Yahoo Finance)
    #
    # Para pares DESDOBRAMENTO+GRUPAMENTO na mesma data, a matemática resolve naturalmente:
    #   TIMS3: 100 × 0.01 = 1.0 (limpeza de base, neutro)
    #   VIVT3: 80 × 0.025 = 2.0 (desdobramento 2:1 real)
    eventos: list[dict] = []
    if not df_bonif.empty and "lastDatePrior" in df_bonif.columns:
        for _, row in df_bonif.iterrows():
            dt = pd.to_datetime(row.get("lastDatePrior", ""), format="%d/%m/%Y", errors="coerce")
            fac = _parse_float(row.get("factor", 0))
            label = str(row.get("label", "")).upper()
            if pd.notna(dt) and pd.notna(fac) and fac != 0 and t0 < dt <= t1:
                if label in ("RESG TOTAL RV", "RESGATE TOTAL RV"):
                    continue
                if label in ("GRUPAMENTO", "SPLIT_YF"):
                    mult = fac
                else:  # BONIFICACAO, DESDOBRAMENTO
                    mult = 1.0 + fac / 100.0
                eventos.append({"date": dt, "mult": round(mult, 8), "factor": fac, "label": row.get("label", "")})
    eventos.sort(key=lambda x: x["date"])

    def mult_ate(data: pd.Timestamp) -> float:
        m = 1.0
        for ev in eventos:
            if ev["date"] <= data:
                m *= ev["mult"]
        return m

    mult_final = mult_ate(t1)

    # mult_yf: apenas eventos do tipo split/desdobramento/grupamento — o YF ajusta dividendos
    # históricos retroativamente para esses eventos mas NÃO para bonificações em ações.
    # Usado exclusivamente no check de divergência com o YF (não no cálculo do TSR).
    _LABELS_SPLIT = {"DESDOBRAMENTO", "GRUPAMENTO", "SPLIT_YF"}
    mult_yf = 1.0
    for ev in eventos:
        if str(ev["label"]).upper() in _LABELS_SPLIT:
            mult_yf *= ev["mult"]

    # --- Dividendos ---
    total_divs = 0.0
    divs_detail: list[dict] = []
    if not df_divs.empty and "value" in df_divs.columns:
        for _, row in df_divs.iterrows():
            dt_ex = pd.to_datetime(row.get("lastDatePriorEx", ""), format="%d/%m/%Y", errors="coerce")
            val = _parse_float(row.get("value", 0))
            if pd.isna(dt_ex) or pd.isna(val):
                continue
            m_div = mult_ate(dt_ex)
            div_total = m_div * val
            total_divs += div_total
            divs_detail.append({
                "Data Ex": row.get("lastDatePriorEx", ""),
                "Pagamento": row.get("paymentDate", ""),
                "Tipo": row.get("label", ""),
                "Valor/Ação (R$)": round(val, 6),
                "Multiplicador": round(m_div, 6),
                "Total Recebido (R$)": round(div_total, 6),
            })

    p_final_adj = p_final * mult_final
    ret_preco = (p_final_adj - p0) / p0
    ret_divs = total_divs / p0
    tsr_total = ret_preco + ret_divs

    return {
        "Ticker": ticker,
        "P0 (R$)": round(p0, 4),
        "P Final (R$)": round(p_final, 4),
        "Mult. Corporativo": round(mult_final, 6),
        "P Final Ajustado (R$)": round(p_final_adj, 4),
        "Dividendos/JCP (R$)": round(total_divs, 4),
        "Ret. Preço (%)": round(ret_preco * 100, 2),
        "Ret. Dividendos (%)": round(ret_divs * 100, 2),
        "TSR Total (%)": round(tsr_total * 100, 2),
        "_divs_detail": divs_detail,
        "_eventos": eventos,
        "_mult_yf": round(mult_yf, 6),
    }


# ---------------------------------------------------------------------------
# VWAP fetcher
# ---------------------------------------------------------------------------

def buscar_vwap_mes(
    tickers: list[str],
    dt_ini: date,
    dt_fim: date,
    logger: Callable[[str], None] = print,
) -> dict[str, tuple[float | None, pd.DataFrame]]:
    """
    Baixa COTAHIST para todos os tickers no período e retorna VWAP mensal.

    Returns:
        {ticker: (vwap_ou_None, df_cotacoes_diarias)}
    """
    logger(f"  Baixando COTAHIST {dt_ini} → {dt_fim} para {len(tickers)} tickers...")
    dias = b3_engine.listar_dias_uteis(dt_ini, dt_fim)
    frames: list[pd.DataFrame] = []

    with requests.Session() as session:
        with ThreadPoolExecutor(max_workers=5) as ex:
            futs = [ex.submit(b3_engine.baixar_e_parsear_dia, d, tickers, session) for d in dias]
            for fut in futs:
                r = fut.result()
                if r is not None:
                    frames.append(r)

    result: dict[str, tuple[float | None, pd.DataFrame]] = {t: (None, pd.DataFrame()) for t in tickers}

    if not frames:
        logger("  Aviso: nenhum dado COTAHIST retornado para o período.")
        return result

    df_all = pd.concat(frames, ignore_index=True)
    df_all["Date"] = pd.to_datetime(df_all["Date"])

    for ticker in tickers:
        df_t = df_all[df_all["Ticker"] == ticker].copy()
        if df_t.empty:
            result[ticker] = (None, pd.DataFrame())
        else:
            df_t = df_t.sort_values("Date").reset_index(drop=True)
            vwap = _calcular_vwap(df_t)
            result[ticker] = (vwap, df_t)

    return result


# ---------------------------------------------------------------------------
# B3 proventos fetchers — pure Python (sem Streamlit)
# ---------------------------------------------------------------------------

_TIPO_ACAO = {"3": "ON", "4": "PN", "5": "PN", "6": "PN", "11": "UNT"}


def _fetch_dividendos_b3(
    ticker: str,
    df_empresas: pd.DataFrame,
    dt_ini: date,
    dt_fim: date,
    logger: Callable[[str], None] = print,
) -> pd.DataFrame:
    """
    Busca dividendos/JCP na B3 para um ticker, filtrando por typeStock e período.
    Não depende de Streamlit — usa logger para output.
    """
    info = ticker_service.get_ticker_info(ticker, df_empresas)
    if not info:
        logger(f"  Aviso: {ticker} não encontrado em df_empresas — sem dividendos.")
        return pd.DataFrame()

    trading_name = info["trading_name"]
    tipo_acao = info["type_stock"]
    if not trading_name or not tipo_acao:
        return pd.DataFrame()

    all_results: list[dict] = []
    current_page = 1
    total_pages = 1

    session = curl_requests.Session(impersonate="chrome120")
    session.headers.update({
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://www.b3.com.br/",
        "Origin": "https://www.b3.com.br",
    })
    try:
        while current_page <= total_pages:
            try:
                params = {
                    "language": "pt-br",
                    "pageNumber": current_page,
                    "pageSize": 60,
                    "tradingName": trading_name,
                }
                encoded = b64encode(json.dumps(params, separators=(",", ":")).encode()).decode()
                url = f"https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetListedCashDividends/{encoded}"
                resp = session.get(url, timeout=30)
                resp.raise_for_status()
                data = resp.json()
                if current_page == 1:
                    total_pages = int(data.get("page", {}).get("totalPages", 1))
                all_results.extend(data.get("results", []))
                if total_pages > 1:
                    time.sleep(0.2)
                current_page += 1
            except Exception as e:
                logger(f"  Erro ao buscar dividendos B3 para {ticker} (pág {current_page}): {e}")
                break
    finally:
        session.close()

    if not all_results:
        return pd.DataFrame()

    df = pd.DataFrame(all_results)

    # Normaliza nomes de colunas: API B3 usa 'valueCash', 'corporateAction', 'dateApproval'
    # mas calcular_tsr espera 'value', 'label', 'paymentDate'
    df = df.rename(columns={
        "valueCash": "value",
        "corporateAction": "label",
        "dateApproval": "paymentDate",
    })

    if "typeStock" in df.columns:
        df["typeStock"] = df["typeStock"].str.strip().str.upper()
        # Ações PN podem ter variantes na API B3: "PN", "PNB" (classe B), "PNC" (classe C), etc.
        # Para tickers sufixo 4/5/6 usamos prefixo "PN"; ON e UNT permanecem com match exato.
        if tipo_acao.startswith("PN"):
            df = df[df["typeStock"].str.startswith("PN")].copy()
        else:
            df = df[df["typeStock"] == tipo_acao].copy()
    if df.empty:
        return pd.DataFrame()

    df["Ticker"] = ticker
    if "lastDatePriorEx" in df.columns:
        df["_dt"] = pd.to_datetime(df["lastDatePriorEx"], format="%d/%m/%Y", errors="coerce")
        df = df.dropna(subset=["_dt"])
        df = df[(df["_dt"] >= pd.Timestamp(dt_ini)) & (df["_dt"] <= pd.Timestamp(dt_fim))]
        df = df.drop(columns=["_dt"])
    else:
        logger(f"  Aviso: coluna 'lastDatePriorEx' ausente para {ticker} — retornando vazio.")
        return pd.DataFrame()
    return df.reset_index(drop=True)


def _fetch_bonificacoes_b3(
    ticker: str,
    df_empresas: pd.DataFrame,
    dt_ini: date,
    dt_fim: date,
    logger: Callable[[str], None] = print,
) -> pd.DataFrame:
    """
    Busca eventos de bonificação/desdobramento/grupamento na B3.
    Não depende de Streamlit.
    """
    info = ticker_service.get_ticker_info(ticker, df_empresas)
    if not info or not info.get("code"):
        logger(f"  Aviso: CODE não encontrado para {ticker} — sem bonificações.")
        return pd.DataFrame()

    code = info["code"]
    session = curl_requests.Session(impersonate="chrome120")
    session.headers.update({
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://www.b3.com.br/",
        "Origin": "https://www.b3.com.br",
    })

    try:
        params = {"issuingCompany": code, "language": "pt-br"}
        encoded = b64encode(json.dumps(params).encode()).decode()
        url = f"https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetListedSupplementCompany/{encoded}"
        resp = session.get(url, timeout=30)
        resp.raise_for_status()

        if not resp.content or not resp.text.strip():
            return pd.DataFrame()

        data = resp.json()
        if not isinstance(data, list) or not data or "stockDividends" not in data[0]:
            return pd.DataFrame()

        df = pd.DataFrame(data[0]["stockDividends"])
        if df.empty:
            return pd.DataFrame()

        dedup_cols = [c for c in ["lastDatePrior", "label"] if c in df.columns]
        if dedup_cols:
            df = df.drop_duplicates(subset=dedup_cols)

        df["Ticker"] = ticker
        if "lastDatePrior" in df.columns:
            df["_dt"] = pd.to_datetime(df["lastDatePrior"], format="%d/%m/%Y", errors="coerce")
            df = df.dropna(subset=["_dt"])
            df = df[
                (df["_dt"] >= pd.Timestamp(dt_ini)) & (df["_dt"] <= pd.Timestamp(dt_fim))
            ].drop(columns=["_dt"])
        else:
            logger(f"  Aviso: coluna 'lastDatePrior' ausente para {ticker} — retornando vazio.")
            return pd.DataFrame()

        cols = ["Ticker", "label", "lastDatePrior", "factor", "approvedIn", "isinCode"]
        existing = [c for c in cols if c in df.columns]
        return df[existing].reset_index(drop=True)

    except Exception as e:
        logger(f"  Erro ao buscar bonificações B3 para {ticker}: {e}")
        return pd.DataFrame()
    finally:
        session.close()


# ---------------------------------------------------------------------------
# Yahoo Finance double-check
# ---------------------------------------------------------------------------

def _fetch_total_proventos_yf(
    ticker: str,
    t0: pd.Timestamp,
    t1: pd.Timestamp,
) -> float:
    """Soma total de dividendos no período via Yahoo Finance."""
    try:
        yf_ticker = f"{ticker}.SA"
        obj = yf.Ticker(yf_ticker)
        divs = obj.dividends
        if divs.empty:
            return 0.0
        divs = divs.reset_index()
        divs.columns = ["Date", "value"]
        divs["Date"] = pd.to_datetime(divs["Date"]).dt.tz_localize(None)
        divs = divs[(divs["Date"] >= t0) & (divs["Date"] <= t1)]
        return float(divs["value"].sum())
    except Exception as e:
        # YF failure treated as 0 dividends; divergence check may produce false positive
        return 0.0


def _detectar_divergencia_yf(
    ticker: str,
    total_b3: float,
    total_yf: float,
    mult_final: float,
    threshold: float,
) -> str | None:
    """
    Detecta divergência real entre dividendos B3 e Yahoo Finance.

    O YF reporta dividendos por ação ATUAL (não ajustado para eventos corporativos),
    enquanto total_b3 já incorpora o multiplicador (dividendos por ação original).
    Para comparar na mesma base, escalamos o YF pelo mult_final:
        total_yf_scaled = total_yf × mult_final

    Casos SPLIT_SCALING (ratio B3/YF ≈ mult_final) deixam de ser falsos positivos.
    """
    total_yf_scaled = total_yf * mult_final
    denom = max(abs(total_b3), abs(total_yf_scaled), 1e-9)
    if abs(total_b3 - total_yf_scaled) / denom > threshold:
        return (
            f"{ticker}: B3={total_b3:.4f} vs YF×{mult_final:.4f}={total_yf_scaled:.4f} "
            f"(divergência {abs(total_b3-total_yf_scaled)/denom*100:.1f}%)"
        )
    return None


# ---------------------------------------------------------------------------
# Grouping
# ---------------------------------------------------------------------------

def _calcular_grupos(ranking: list[TickerResult]) -> dict[int, list[TickerResult]]:
    """
    Divide ranking em 6 grupos ordenados por TSR desc.
    Tamanho base q = N // 6; primeiros r = N % 6 grupos têm q+1 elementos.
    Atribui .rank e .grupo em cada TickerResult.
    """
    n = len(ranking)
    if n == 0:
        return {g: [] for g in range(1, 7)}
    q, r = divmod(n, 6)
    grupos: dict[int, list[TickerResult]] = {}
    idx = 0
    for g in range(1, 7):
        size = q + (1 if g <= r else 0)
        grupo_tickers = ranking[idx: idx + size]
        for local_idx, t in enumerate(grupo_tickers):
            t.rank = idx + local_idx + 1
            t.grupo = g
        grupos[g] = grupo_tickers
        idx += size
    return grupos


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def calcular_outorga(
    config: OutorgaConfig,
    df_empresas: pd.DataFrame,
    logger: Callable[[str], None] = print,
) -> ApuracaoResult:
    """
    Calcula TSR batch para todos os tickers da outorga seguindo o Book de Regras.
    """
    logger(f"\n{'='*60}")
    logger(f"Outorga {config.ano} | {len(config.tickers)} tickers | "
           f"P0: {config.dt_p0_ini}–{config.dt_p0_fim} | "
           f"Pf: {config.dt_pf_ini}–{config.dt_pf_fim}")
    logger(f"{'='*60}")

    # ── Pré-step: detectar renomeações via COTAHIST para tickers sem config manual ──
    tickers_sem_config = [
        t for t in config.tickers
        if t not in config.exclusoes_forcadas and t not in config.substituicoes
    ]
    substituicoes_auto: dict[str, str] = {}
    if tickers_sem_config:
        logger("  Verificando tickers no período Pf via COTAHIST...")
        dias_p0 = b3_engine.listar_dias_uteis(config.dt_p0_ini, config.dt_p0_fim)
        dias_pf = b3_engine.listar_dias_uteis(config.dt_pf_ini, config.dt_pf_fim)
        dt_p0_amostra = dias_p0[-1] if dias_p0 else config.dt_p0_fim
        dt_pf_amostra = dias_pf[-1] if dias_pf else config.dt_pf_fim
        with requests.Session() as sess_det:
            deteccoes = b3_engine.detectar_substituicoes_cotahist(
                tickers=tickers_sem_config,
                dt_origem=dt_p0_amostra,
                dt_alvo=dt_pf_amostra,
                session=sess_det,
            )
        for t, info in deteccoes.items():
            if info["substituto"]:
                substituicoes_auto[t] = info["substituto"]
                logger(f"    Auto: {t} → {info['substituto']} "
                       f"({info['metodo']}) [{info['nome_orig']} → {info['nome_subst']}]")
            else:
                logger(f"    Aviso: {t} ausente em Pf, sem substituto detectado "
                       f"(nome: {info['nome_orig']}) — será SEM_DADOS se não houver config")

    # Merge: config.substituicoes tem prioridade sobre auto-detectado
    substituicoes_efetivas: dict[str, str] = {**substituicoes_auto, **config.substituicoes}

    # Tickers efetivos para Pf (aplica substituições efetivas)
    tickers_p0 = config.tickers[:]
    tickers_pf = [substituicoes_efetivas.get(t, t) for t in config.tickers]

    # Batch VWAP download
    logger("Baixando VWAP P0...")
    vwap_p0_map = buscar_vwap_mes(tickers_p0, config.dt_p0_ini, config.dt_p0_fim, logger)
    logger("Baixando VWAP P_final...")
    vwap_pf_map = buscar_vwap_mes(
        list(set(tickers_pf)), config.dt_pf_ini, config.dt_pf_fim, logger
    )

    t0 = pd.Timestamp(config.dt_divs_ini)
    t1 = pd.Timestamp(config.dt_divs_fim)

    resultados: list[TickerResult] = []

    for ticker_orig in config.tickers:
        ticker_ef = substituicoes_efetivas.get(ticker_orig, ticker_orig)
        logger(f"\n  [{ticker_orig}→{ticker_ef}]" if ticker_ef != ticker_orig else f"\n  [{ticker_orig}]")

        # Excluídos forçados
        if ticker_orig in config.exclusoes_forcadas:
            logger(f"    Excluído forçado (config)")
            resultados.append(TickerResult(
                ticker=ticker_orig, ticker_original=ticker_orig,
                vwap_p0=None, vwap_pf=None, dividendos_total=0.0,
                status="EXCLUIDO_FORCADO", motivo_exclusao="exclusao_forcada em config",
            ))
            continue

        vwap_p0, df_cot_p0 = vwap_p0_map.get(ticker_orig, (None, pd.DataFrame()))
        vwap_pf, df_cot_pf = vwap_pf_map.get(ticker_ef, (None, pd.DataFrame()))

        if vwap_p0 is None or vwap_pf is None:
            motivo = []
            if vwap_p0 is None:
                motivo.append("sem_dados_P0")
            if vwap_pf is None:
                motivo.append("sem_dados_Pf")
            logger(f"    Sem dados COTAHIST: {', '.join(motivo)}")
            resultados.append(TickerResult(
                ticker=ticker_ef, ticker_original=ticker_orig,
                vwap_p0=vwap_p0, vwap_pf=vwap_pf, dividendos_total=0.0,
                status="SEM_DADOS", motivo_exclusao=", ".join(motivo),
                df_cotacoes_p0=df_cot_p0, df_cotacoes_pf=df_cot_pf,
            ))
            continue

        logger(f"    VWAP P0={vwap_p0:.4f}  VWAP Pf={vwap_pf:.4f}")

        # Proventos B3
        df_divs = _fetch_dividendos_b3(ticker_ef, df_empresas, config.dt_divs_ini, config.dt_divs_fim, logger)
        df_bonif = _fetch_bonificacoes_b3(ticker_ef, df_empresas, config.dt_divs_ini, config.dt_divs_fim, logger)

        n_divs = len(df_divs) if not df_divs.empty else 0
        n_bonif = len(df_bonif) if not df_bonif.empty else 0
        logger(f"    Dividendos B3: {n_divs}  |  Eventos corporativos: {n_bonif}")

        # Cálculo TSR
        tsr_dict = calcular_tsr(ticker_ef, vwap_p0, vwap_pf, df_divs, df_bonif, t0, t1)
        tsr_decimal = tsr_dict["TSR Total (%)"] / 100

        # Yahoo Finance double-check
        # Para tickers renomeados usa o ticker original (YF mantém dados históricos sob o nome antigo)
        yf_lookup_ticker = ticker_orig if ticker_orig != ticker_ef else ticker_ef
        total_yf = _fetch_total_proventos_yf(yf_lookup_ticker, t0, t1)
        total_b3 = tsr_dict["Dividendos/JCP (R$)"]
        # Usa mult_yf (apenas splits/desdobramentos/grupamentos) pois o YF ajusta retroativamente
        # apenas esses eventos; bonificações em ações não são ajustadas pelo YF.
        mult_yf = tsr_dict["_mult_yf"]
        divergencia = _detectar_divergencia_yf(ticker_ef, total_b3, total_yf, mult_yf, config.divergencia_threshold)
        if divergencia:
            logger(f"    AVISO divergência YF: {divergencia}")

        resultados.append(TickerResult(
            ticker=ticker_ef,
            ticker_original=ticker_orig,
            vwap_p0=vwap_p0,
            vwap_pf=vwap_pf,
            dividendos_total=total_b3,
            eventos_corporativos=tsr_dict["_eventos"],
            divs_ajustados=tsr_dict["_divs_detail"],
            tsr=tsr_decimal,
            mult_corporativo=tsr_dict["Mult. Corporativo"],
            status="INCLUIDO",
            divergencia_yf=divergencia,
            df_cotacoes_p0=df_cot_p0,
            df_cotacoes_pf=df_cot_pf,
            df_dividendos=df_divs,
            df_bonificacoes=df_bonif,
        ))

    # Ranking e grupos
    incluidos = sorted(
        [r for r in resultados if r.status == "INCLUIDO"],
        key=lambda x: x.tsr or -999,
        reverse=True,
    )
    grupos = _calcular_grupos(incluidos)

    return ApuracaoResult(
        outorga=config,
        tickers=resultados,
        ranking=incluidos,
        grupos=grupos,
        n_incluidos=len(incluidos),
        n_excluidos=len(resultados) - len(incluidos),
        timestamp=datetime.now(),
    )


def calcular_todas_outorgas(
    anos: list[int],
    df_empresas: pd.DataFrame,
    logger: Callable[[str], None] = print,
) -> dict[int, ApuracaoResult]:
    """Calcula múltiplas outorgas e retorna dict {ano: ApuracaoResult}."""
    result = {}
    for ano in anos:
        if ano not in OUTORGAS:
            logger(f"Aviso: outorga {ano} não configurada em OUTORGAS — ignorando.")
            continue
        result[ano] = calcular_outorga(OUTORGAS[ano], df_empresas, logger)
    return result
